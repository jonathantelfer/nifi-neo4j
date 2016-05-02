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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.*;


@Tags({ "neo4j", "bolt", "connection", "pool", "graph", "database"})
@CapabilityDescription("Provides Neo4j database session pooling service using the Bolt protocol driver."
    +" Sessions can be requested from pool and must be returned after usage. This service requires Neo4j 3.0")
public class Neo4jBoltSessionPool extends AbstractControllerService implements BoltSessionPool {

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
        .name("Bolt Database Connection URL")
        .description("A Bolt database URL to connect to Neo4j. Should be in the format bolt://hostname:port."
            + " If no port is provided in the URL, the default port 7687 is used.")
        .defaultValue("bolt://localhost")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build();

    public static final PropertyDescriptor MAX_TOTAL_SESSIONS = new PropertyDescriptor.Builder()
        .name("Max Total Sessions")
        .description("The maximum number of active sessions that can be allocated from this pool at the same time.")
        .defaultValue("50")
        .required(true)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .sensitive(false)
        .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
        .name("Database User")
        .description("Database user name")
        .defaultValue(null)
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
        .name("Password")
        .description("The password for the database user")
        .defaultValue(null)
        .required(true)
        .sensitive(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();


    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(MAX_TOTAL_SESSIONS);
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        properties = Collections.unmodifiableList(props);
    }

    private volatile Driver driver;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link BasicDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     *
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        final String dburl = context.getProperty(DATABASE_URL).getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_SESSIONS).asInteger();
        final String user = context.getProperty(DB_USER).getValue();
        final String passw = context.getProperty(DB_PASSWORD).getValue();
        // final Long maxWaitMillis = context.getProperty(MAX_WAIT_TIME).asTimePeriod(TimeUnit.MILLISECONDS);

        driver = GraphDatabase.driver(dburl,
                                      AuthTokens.basic(user, passw),
                                      Config.build().withMaxSessions(maxTotal).toConfig() );
    }

    @OnDisabled
    public void shutdown() {
        try {
            driver.close();
        } catch (final Neo4jException e) {
            throw new ProcessException(e);
        }
    }

    public Session getSession() {
        return driver.session();
    }

    @Override
    public String toString() {
        return "Neo4jBoltSessionPoolService[id=" + getIdentifier() + "]";
    }
}
