/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.base;

import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.baidu.hugegraph.driver.AuthManager;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.GremlinManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.driver.TaskManager;
import com.baidu.hugegraph.driver.TraverserManager;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.driver.factory.MetaHugeClientFactory;

public class ToolClient {

    private static final String DEFAULT_TRUST_STORE_FILE =
                                "conf/hugegraph.truststore";
    private static final String DEFAULT_TRUST_STORE_PASSWORD = "hugegraph";

    private HugeClient client;
    private ObjectMapper mapper;

    public ToolClient(ConnectionInfo info) {
        if (info.username == null) {
            info.username = "";
            info.password = "";
        }

        if (StringUtils.isNotEmpty(info.url)) {
            String trustStoreFile;
            String trustStorePassword;
            if (info.url.startsWith("https")) {
                if (info.trustStoreFile == null || info.trustStoreFile.isEmpty()) {
                    trustStoreFile = Paths.get(homePath(),
                                               DEFAULT_TRUST_STORE_FILE)
                                          .toString();
                    trustStorePassword = DEFAULT_TRUST_STORE_PASSWORD;
                } else {
                    E.checkArgumentNotNull(info.trustStorePassword,
                                           "The trust store password can't be " +
                                                   "null when use https");
                    trustStoreFile = info.trustStoreFile;
                    trustStorePassword = info.trustStorePassword;
                }
            } else {
                assert info.url.startsWith("http");
                E.checkArgument(info.trustStoreFile == null,
                                "Can't set --trust-store-file when use http");
                E.checkArgument(info.trustStorePassword == null,
                                "Can't set --trust-store-password when use http");
                trustStoreFile = info.trustStoreFile;
                trustStorePassword = info.trustStorePassword;
            }
            this.client = HugeClient.builder(info.url, info.graphSpace,
                                             info.graph)
                                    .configUser(info.username, info.password)
                                    .configTimeout(info.timeout)
                                    .configSSL(trustStoreFile,
                                               trustStorePassword)
                                    .build();
        } else {
            this.createHugeClientWithMeta(info);
        }

        this.mapper = new ObjectMapper();
    }

    protected void createHugeClientWithMeta(ConnectionInfo info) {
        E.checkArgument(CollectionUtils.isNotEmpty(info.metaURLs),
                        "The endpoints can't be null, when use meta");
        MetaHugeClientFactory.MetaDriverType type
                = MetaHugeClientFactory.MetaDriverType.valueOf(
                info.metaType.toUpperCase());
        MetaHugeClientFactory factory = MetaHugeClientFactory
                .connect(type, info.metaURLs.toArray(new String[0]),
                         info.metaCa, info.metaClientCa,
                         info.metaClientKey);
        try {
            client = factory.createAuthClient(info.cluster, info.graphSpace,
                                              info.graph, null, info.username,
                                              info.password);
        } catch (RuntimeException e) {
            throw e;
        } finally {
            factory.close();
        }
    }

    public TraverserManager traverser() {
        return this.client.traverser();
    }

    public GraphManager graph() {
        return this.client.graph();
    }

    public SchemaManager schema() {
        return this.client.schema();
    }

    public com.baidu.hugegraph.driver.GraphsManager graphs() {
        return this.client.graphs();
    }

    public TaskManager tasks() {
        return this.client.task();
    }

    public GremlinManager gremlin() {
        return this.client.gremlin();
    }

    public ObjectMapper mapper() {
        return this.mapper;
    }

    public static String homePath() {
        String homePath = System.getProperty("tools.home.path");
        E.checkArgument(StringUtils.isNotEmpty(homePath),
                        "The system property 'tools.home.path' " +
                        "can't be empty when enable https protocol");
        return homePath;
    }

    public AuthManager authManager() {
        return this.client.auth();
    }

    public void close() {
        if (this.client != null) {
            this.client.close();
        }
    }

    public static class ConnectionInfo {

        private String url;
        private String metaType;
        private List<String> metaURLs;
        private String metaCa;
        private String metaClientCa;
        private String metaClientKey;
        private String cluster;
        private String graphSpace;
        private String graph;
        private String username;
        private String password;
        private Integer timeout;
        private String trustStoreFile;
        private String trustStorePassword;

        public ConnectionInfo(String url, String graphSpace, String graph,
                              String username, String password,
                              Integer timeout, String trustStoreFile,
                              String trustStorePassword) {
            this.url = url;
            this.graphSpace = graphSpace;
            this.graph = graph;
            this.username = username;
            this.password = password;
            this.timeout = timeout;
            this.trustStoreFile = trustStoreFile;
            this.trustStorePassword = trustStorePassword;
        }

        public ConnectionInfo(String metaType, List<String> metaURLs,
                              String metaCa, String metaClientCA,
                              String metaClientKey, String cluster,
                              String graphSpace, String graph, String username,
                              String password, Integer timeout) {
            this.metaType = metaType;
            this.metaURLs = metaURLs;
            this.cluster = cluster;
            this.graphSpace = graphSpace;
            this.graph = graph;
            this.username = username;
            this.password = password;
            this.timeout = timeout;
            this.metaCa = metaCa;
            this.metaClientCa = metaClientCA;
            this.metaClientKey = metaClientKey;
        }
    }
}
