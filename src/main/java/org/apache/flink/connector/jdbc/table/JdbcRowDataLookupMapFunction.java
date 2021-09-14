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

import com.cloudera.impala.jdbc.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.runtime.tasks.ExceptionInChainedOperatorException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.jdbc.internal.options.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SECONDS;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A lookup function for {@link JdbcDynamicTableSource}.
 */
@Internal
public class JdbcRowDataLookupMapFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataLookupMapFunction.class);
    private static final long serialVersionUID = 1L;

    private final String query;
    private final String drivername;
    private final String dbURL;
    private final String username;
    private final String password;
    private final DataType[] keyTypes;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final JdbcRowConverter lookupKeyRowConverter;

    private transient Connection dbConn;
    private transient DataSource ds;
    private transient PreparedStatement statement;
    private transient MapState<RowData, RowData> infoState;
    private transient Cache<RowData, List<RowData>> cache;

    public JdbcRowDataLookupMapFunction(
            JdbcOptions options,
            JdbcLookupOptions lookupOptions,
            String[] fieldNames,
            DataType[] fieldTypes,
            String[] keyNames,
            RowType rowType) {
        checkNotNull(options, "No JdbcOptions supplied.");
        checkNotNull(fieldNames, "No fieldNames supplied.");
        checkNotNull(fieldTypes, "No fieldTypes supplied.");
        checkNotNull(keyNames, "No keyNames supplied.");
        this.drivername = options.getDriverName();
        this.dbURL = options.getDbURL();
        this.username = options.getUsername().orElse(null);
        this.password = options.getPassword().orElse(null);
        List<String> nameList = Arrays.asList(fieldNames);
        this.keyTypes = Arrays.stream(keyNames)
                .map(s -> {
                    checkArgument(nameList.contains(s),
                            "keyName %s can't find in fieldNames %s.", s, nameList);
                    return fieldTypes[nameList.indexOf(s)];
                })
                .toArray(DataType[]::new);
        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();
        this.maxRetryTimes = lookupOptions.getMaxRetryTimes();
        this.query = options.getDialect().getSelectFromStatement(
                options.getTableName(), fieldNames, keyNames);
        this.jdbcDialect = JdbcDialects.get(dbURL)
                .orElseThrow(() -> new UnsupportedOperationException(String.format("Unknown dbUrl:%s", dbURL)));
        this.jdbcRowConverter = jdbcDialect.getRowConverter(rowType);
        this.lookupKeyRowConverter = jdbcDialect.getRowConverter(RowType.of(Arrays.stream(keyTypes).map(DataType::getLogicalType).toArray(LogicalType[]::new)));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        try {
            establishConnectionAndStatement();
            this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
    }

    /**
     * This is a lookup method which is called by Flink framework in runtime.
     *
     * @param keys lookup keys
     */
    public void eval(Object... keys) {
        RowData keyRow = GenericRowData.of(keys);
        if (cache != null) {
            List<RowData> cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {
                cachedRows.forEach(cachedRow -> {
                    collect(cachedRow);
                });
                return;
            }
        }

        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            try {
                statement.clearParameters();
                statement = lookupKeyRowConverter.toExternal(keyRow, statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (cache == null) {
                        while (resultSet.next()) {
                            collect(jdbcRowConverter.toInternal(resultSet));
                        }
                    } else {
                        ArrayList<RowData> rows = new ArrayList<>();
                        while (resultSet.next()) {
                            RowData row = jdbcRowConverter.toInternal(resultSet);
                            if (row != null && StringUtils.isNotEmpty(row.toString())) {
                                rows.add(row);
                                collect(row);
                            }
                        }
                        rows.trimToSize();
                        cache.put(keyRow, rows);
                    }
                }
                break;
            } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
          /*      if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }*/

                try {
                    if (!dbConn.isValid(CONNECTION_CHECK_TIMEOUT_SECONDS)) {
                        statement.close();
                        dbConn.close();
                        establishConnectionAndStatement();
                    }
                } catch (SQLException | ClassNotFoundException excpetion) {
                    LOG.error("JDBC connection is not valid, and reestablish connection failed", excpetion);
                    throw new RuntimeException("Reestablish JDBC connection failed", excpetion);
                } catch (Exception exception) {
                    throw new RuntimeException("Reestablish JDBC connection failed", exception);                }

                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            } catch (ExceptionInChainedOperatorException e) {
                LOG.warn("数据异常:  sql:" + query + ",keyROW:" + keyRow.toString());

            } catch (Exception e) {
                LOG.warn("数据异常:  sql:" + query + ",keyROW:" + keyRow.toString());

            }
        }
    }

    private Connection connectViaDS() throws Exception {
        Connection connection = null;
        if (ds == null) {
            Class.forName("com.cloudera.impala.jdbc.DataSource");
            ds = new DataSource();
            ds.setURL(dbURL);
        }
        dbConn = ds.getConnection();
        return dbConn;
    }

    private void establishConnectionAndStatement() throws Exception {

/*        int size = (int) ((cacheExpireMs / 1000) * 2);
        DriverManager.setLoginTimeout(size);*/
       if (drivername.contains("com.cloudera.impala.jdbc."))
            this.connectViaDS();
       else
           Class.forName(drivername);
        if (username == null)
            dbConn = DriverManager.getConnection(dbURL);

        else {
            dbConn = DriverManager.getConnection(dbURL, username, password);
        }
        statement = dbConn.prepareStatement(query);
    }

    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }

        } catch (SQLException e) {
            LOG.info("JDBC statement could not be closed: " + e.getMessage());
        } finally {
            statement = null;
        }

        try {
            if (dbConn != null && !dbConn.isClosed())

                dbConn.close();

        } catch (SQLException se) {
            LOG.info("JDBC connection could not be closed: " + se.getMessage());
        } finally {
            dbConn = null;
        }

    }

    @VisibleForTesting
    public Connection getDbConnection() {
        return dbConn;
    }
}
