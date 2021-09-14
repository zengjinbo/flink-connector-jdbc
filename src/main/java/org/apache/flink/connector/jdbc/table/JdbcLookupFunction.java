/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.table;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.jdbc.internal.options.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SECONDS;
import static org.apache.flink.connector.jdbc.utils.JdbcUtils.getFieldFromResultSet;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TableFunction} to query fields from JDBC by keys.
 * The query template like:
 * <PRE>
 * SELECT c, d, e, f from T where a = ? and b = ?
 * </PRE>
 *
 * <p>Support cache the result to avoid frequent accessing to remote databases.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 */
public class JdbcLookupFunction extends TableFunction<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final String query;
    private final String queryWhere;
    private final String drivername;
    private final String dbURL;
    private final String username;
    private final String password;
    private final TypeInformation[] keyTypes;
    private final int[] keySqlTypes;
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;
    private final int[] outputSqlTypes;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final String[] keyNames;
    private transient Connection dbConn;
    private transient PreparedStatement statement;
    private transient Cache<Row, Row> cache;
    private final ScheduledExecutorService scheduledThreadPool ;

    public JdbcLookupFunction(
            JdbcOptions options, JdbcLookupOptions lookupOptions,
            String[] fieldNames, TypeInformation[] fieldTypes, String[] keyNames) {
        this.drivername = options.getDriverName();
        this.dbURL = options.getDbURL();
        this.username = options.getUsername().orElse(null);
        this.password = options.getPassword().orElse(null);
        this.fieldNames = fieldNames;
        this.keyNames = keyNames;
        this.fieldTypes = fieldTypes;
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyTypes = Arrays.stream(keyNames)
                .map(s -> {
                    checkArgument(nameList.contains(s),
                            "keyName %s can't find in fieldNames %s.", s, nameList);
                    return fieldTypes[nameList.indexOf(s)];
                })
                .toArray(TypeInformation[]::new);
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.keySqlTypes = Arrays.stream(keyTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();
        this.outputSqlTypes = Arrays.stream(fieldTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();
        this.query = options.getDialect().getSelectFromStatement(
                options.getTableName(), fieldNames, null);
        this.queryWhere = options.getDialect().getSelectFromStatement(
                options.getTableName(), fieldNames, keyNames);
        this.scheduledThreadPool = Executors.newScheduledThreadPool(1);

    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void open(FunctionContext context) throws Exception {


        this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireMs + (60 * 1000), TimeUnit.MILLISECONDS)
                .maximumSize(cacheMaxSize)
                .build();
        scheduledThreadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                getCache();
            }
        }, cacheExpireMs, cacheExpireMs, TimeUnit.MILLISECONDS);
        getCache();

    }

    public void eval(Object... keys) {
        Row keyRow = Row.of(keys);
        if (cache != null) {
            Row cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                collect(cachedRows);

                return;
            } else {
                cachedRows= getCache(this.queryWhere, keys);
                if (cachedRows!=null)
                    collect(cachedRows);

            }
        }


    }
    public  void getCache() {
        getCache(null, null);
    }
    public synchronized Row getCache(String sql, Object[] keys) {
        Row row = null;
        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            try {
                Connection connection = establishConnectionAndStatement();
                PreparedStatement statement;
                if (StringUtils.isNotEmpty(sql))
                    statement = connection.prepareStatement(sql);
                else
                    statement = connection.prepareStatement(query);
                if (keys != null && keys.length > 0) {
                    for (int i = 0; i < keys.length; i++) {
                        JdbcUtils.setField(statement, keySqlTypes[i], keys[i], i);
                    }
                }
                ResultSet resultSet = statement.executeQuery();
                row= getCache(resultSet);
                if (statement != null&&!statement.isClosed()) {
                    try {
                        statement.close();
                    } catch (SQLException e) {
                        LOG.info("JDBC statement could not be closed: " + e.getMessage());
                    } finally {
                        statement = null;
                    }
                }

                if (connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException se) {
                        LOG.info("JDBC connection could not be closed: " + se.getMessage());
                    } finally {
                        dbConn = null;
                    }
                }

            } catch (Exception e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }

                try {

                    if (!dbConn.isValid(CONNECTION_CHECK_TIMEOUT_SECONDS))
                        this.connClose();
                    establishConnectionAndStatement();

                } catch (SQLException | ClassNotFoundException excpetion) {
                    LOG.error("JDBC connection is not valid, and reestablish connection failed", excpetion);
                    throw new RuntimeException("Reestablish JDBC connection failed", excpetion);
                }

                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        }
        return row;
    }

    public  Row getCache(ResultSet resultSet) throws SQLException {
        Row row = null;
        while (resultSet.next()) {
            row = convertToRowFromResultSet(resultSet);
            Object[] keys = convertTokeyRowFromResultSet(resultSet);
            cache.put(Row.of(keys), row);
        }
        return row;

    }

    private Row convertToRowFromResultSet(ResultSet resultSet) throws SQLException {
        Row row = new Row(outputSqlTypes.length);
        for (int i = 0; i < outputSqlTypes.length; i++) {
            row.setField(i, getFieldFromResultSet(i, outputSqlTypes[i], resultSet));
        }
        return row;
    }

    private Object[] convertTokeyRowFromResultSet(ResultSet resultSet) throws SQLException {
        ArrayList<Object> keys = new ArrayList();
        ResultSetMetaData m = resultSet.getMetaData();
        for (int i = 0; i < m.getColumnCount(); i++) {
            for (int y = 0; y < keyTypes.length; y++) {
                if (keyNames[y].equals(m.getColumnName(i))) {
                    keys.add(resultSet.getObject(i + 1));
                    break;
                }
            }
        }

        return keys.toArray();
    }

    private Connection establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Class.forName(drivername);
        if (username == null) {
            dbConn = DriverManager.getConnection(dbURL);
        } else {
            dbConn = DriverManager.getConnection(dbURL, username, password);
        }
        return dbConn;
    }

    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
        this.connClose();

    }

    public synchronized void connClose() {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.info("JDBC statement could not be closed: " + e.getMessage());
            } finally {
                statement = null;
            }
        }

        if (dbConn != null) {
            try {
                dbConn.close();
            } catch (SQLException se) {
                LOG.info("JDBC connection could not be closed: " + se.getMessage());
            } finally {
                dbConn = null;
            }
        }
    }

    @VisibleForTesting
    public Connection getDbConnection() {
        return dbConn;
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
        return keyTypes;
    }

    /**
     * Builder for a {@link JdbcLookupFunction}.
     */
    public static class Builder {
        private JdbcOptions options;
        private JdbcLookupOptions lookupOptions;
        protected String[] fieldNames;
        protected TypeInformation[] fieldTypes;
        protected String[] keyNames;

        /**
         * required, jdbc options.
         */
        public Builder setOptions(JdbcOptions options) {
            this.options = options;
            return this;
        }

        /**
         * optional, lookup related options.
         */
        public Builder setLookupOptions(JdbcLookupOptions lookupOptions) {
            this.lookupOptions = lookupOptions;
            return this;
        }

        /**
         * required, field names of this jdbc table.
         */
        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        /**
         * required, field types of this jdbc table.
         */
        public Builder setFieldTypes(TypeInformation[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        /**
         * required, key names to query this jdbc table.
         */
        public Builder setKeyNames(String[] keyNames) {
            this.keyNames = keyNames;
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JdbcLookupFunction
         */
        public JdbcLookupFunction build() {
            checkNotNull(options, "No JdbcOptions supplied.");
            if (lookupOptions == null) {
                lookupOptions = JdbcLookupOptions.builder().build();
            }
            checkNotNull(fieldNames, "No fieldNames supplied.");
            checkNotNull(fieldTypes, "No fieldTypes supplied.");
            checkNotNull(keyNames, "No keyNames supplied.");

            return new JdbcLookupFunction(options, lookupOptions, fieldNames, fieldTypes, keyNames);
        }
    }
}
