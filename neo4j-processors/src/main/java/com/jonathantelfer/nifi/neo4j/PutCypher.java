/*
*  Copyright 2016 Jonathan Telfer
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/

package com.jonathantelfer.nifi.neo4j;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.*;

import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.TransientException;
import org.neo4j.driver.v1.summary.SummaryCounters;

//@SeeAlso({GetCypher.class, Neo4jBoltSessionPool.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"cypher", "put", "graph", "query", "update", "insert", "neo4j", "database"})
@CapabilityDescription("Executes a cypher statement to insert or update data in Neo4j. The content of an incoming FlowFile is expected to be the cypher command " +
    "to execute. The cypher query can optionally be provided as an attribute. If the query returns a result the processor will add any" +
    "fields returned as attributes. Only the first result record will be read, any others will be discarded.")
@DynamicProperty(name = "Query parameter", value = "Attribute Expression Language", supportsExpressionLanguage = true, description = "Allows cypher query parameters "
        + "to be specified as properties. Properties should be named cypher.param.type.name where type is one of string, integer, " +
        "float or boolean and name is the name of the query parameter.")
@WritesAttributes({
    @WritesAttribute(attribute = "neo4j.result.*", description = "If the query returns a record an attribute will be created " +
            "for each field in the record. "),
    @WritesAttribute(attribute = "neo4j.error.code", description = "Written in the event of an error."),
    @WritesAttribute(attribute = "neo4j.nodes.created", description = "The number of nodes created."),
    @WritesAttribute(attribute = "neo4j.nodes.deleted", description = "The number of nodes deleted."),
    @WritesAttribute(attribute = "neo4j.relationships.created", description = "The number of relationships created."),
    @WritesAttribute(attribute = "neo4j.relationships.deleted", description = "The number of relationships deleted."),
    @WritesAttribute(attribute = "neo4j.properties.set", description = "The number of properties set."),
    @WritesAttribute(attribute = "neo4j.labels.added", description = "The number of labels added."),
    @WritesAttribute(attribute = "neo4j.labels.removed", description = "The number of labels removed."),
    @WritesAttribute(attribute = "neo4j.indexes.added", description = "The number of indexes added."),
    @WritesAttribute(attribute = "neo4j.indexes.removed", description = "The number of indexes removed."),
    @WritesAttribute(attribute = "neo4j.constraints.added", description = "The number of constraints added."),
    @WritesAttribute(attribute = "neo4j.constraints.removed", description = "The number of constraints removed."),
})

public class PutCypher extends AbstractProcessor {

    static final PropertyDescriptor SESSION_POOL = new PropertyDescriptor.Builder()
        .name("Neo4j Bolt Session Pool")
        .description("Specifies the Bolt Session Pool to use.")
        .identifiesControllerService(BoltSessionPool.class)
        .required(true)
        .build();
    public static final PropertyDescriptor CYPHER_QUERY = new PropertyDescriptor.Builder()
        .name("Cypher query")
        .description("If present, the content of this property will be used as the cypher query rather than the flowFile content")
        .required(false)
        .expressionLanguageSupported(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile is routed to this relationship after the database is successfully updated")
        .build();
    static final Relationship REL_RETRY = new Relationship.Builder()
        .name("retry")
        .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
            + "such as an invalid query or an integrity constraint violation")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SESSION_POOL);
        properties.add(CYPHER_QUERY);
        return properties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_RETRY);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ProcessorLog logger = getLogger();

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final BoltSessionPool sessionPool = context.getProperty(SESSION_POOL).asControllerService(BoltSessionPool.class);
        String cypherQuery = context.getProperty(CYPHER_QUERY).evaluateAttributeExpressions(flowFile).getValue();
        Map<String,Object> params = getQueryParameters(flowFile, context);

        // If the cypher query attribute wasn't set read the query from the flowFile content
        if (cypherQuery == null) {
            final byte[] buffer = new byte[(int) flowFile.getSize()];
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream inputStream) throws IOException {
                    StreamUtils.fillBuffer(inputStream, buffer);
                }
            });
            cypherQuery = new String(buffer);
        }

        try (final Session dbSession = sessionPool.getSession()) {
            StatementResult result = dbSession.run(cypherQuery, params);

            if (result.hasNext()) {
                Record record = result.next();
                List<String> keys = record.keys();
                for(String key : keys) {
                    Object value = record.get(key).asObject();
                    if (value != null) {
                        flowFile = session.putAttribute(flowFile, "neo4j.result." + key, value.toString());
                    }
                }
            }

            SummaryCounters counters = result.consume().counters();

            flowFile = session.putAttribute(flowFile, "neo4j.nodes.created", Integer.toString(counters.nodesCreated()));
            flowFile = session.putAttribute(flowFile, "neo4j.nodes.deleted", Integer.toString(counters.nodesDeleted()));
            flowFile = session.putAttribute(flowFile, "neo4j.relationships.created", Integer.toString(counters.relationshipsCreated()));
            flowFile = session.putAttribute(flowFile, "neo4j.relationships.deleted", Integer.toString(counters.relationshipsDeleted()));
            flowFile = session.putAttribute(flowFile, "neo4j.properties.set", Integer.toString(counters.propertiesSet()));
            flowFile = session.putAttribute(flowFile, "neo4j.labels.added", Integer.toString(counters.labelsAdded()));
            flowFile = session.putAttribute(flowFile, "neo4j.labels.removed", Integer.toString(counters.labelsRemoved()));
            flowFile = session.putAttribute(flowFile, "neo4j.indexes.added", Integer.toString(counters.indexesAdded()));
            flowFile = session.putAttribute(flowFile, "neo4j.indexes.removed", Integer.toString(counters.indexesRemoved()));
            flowFile = session.putAttribute(flowFile, "neo4j.constraints.added", Integer.toString(counters.constraintsAdded()));
            flowFile = session.putAttribute(flowFile, "neo4j.constraints.removed", Integer.toString(counters.constraintsRemoved()));

            session.transfer(flowFile, REL_SUCCESS);
        } catch (ClientException | DatabaseException e) {
            logger.error(e.toString());
            flowFile = session.putAttribute(flowFile, "neo4j.error.code", e.neo4jErrorCode());
            session.transfer(flowFile, REL_FAILURE);
        } catch (TransientException e) {
            logger.info(e.toString());
            flowFile = session.putAttribute(flowFile, "neo4j.error.code", e.neo4jErrorCode());
            session.transfer(flowFile, REL_RETRY);
        }

    }

    /**
     * Retrieves any dynamic properties set on the processor to be used as cypher query parameters.
     *
     * Properties need to be named cypher.param.type.name where type is either boolean, string, integer
     * or float and name is the name of the query parameter.
     *
     * boolean, integer and float parameters will be converted to primitive wrapper objects.
     *
     * @param context the process context for retrieving properties
     * @return a map of query parameters ready to pass to Neo4j
     */
    private Map<String,Object> getQueryParameters(final FlowFile flowFile, final ProcessContext context) {
        Map<String,Object> params = new HashMap<String,Object>();
        Pattern p = Pattern.compile("cypher\\.param\\.(boolean|string|integer|float)\\.(.*)");
        for (Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            Matcher m = p.matcher(property.getKey().getName());
            if (m.matches()) {
                String value = context.getProperty(property.getKey()).evaluateAttributeExpressions(flowFile).getValue();
                switch (m.group(1)) {
                    case "boolean":
                        params.put(m.group(2), new Boolean(value));
                        break;
                    case "integer":
                        params.put(m.group(2), new Integer(value));
                        break;
                    case "float":
                        params.put(m.group(2), new Double(value));
                        break;
                    case "string":
                        params.put(m.group(2), value);
                        break;
                    default:
                        throw new IllegalArgumentException("Unrecognised type " + m.group(1) + " in cypher parameter property " + property.getKey().getName());
                }
            }
        }
        return params;
    }
}
