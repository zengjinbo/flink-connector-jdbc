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

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.jdbc.internal.options.JdbcOptions.CONNECTION_CHECK_TIMEOUT_SECONDS;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A lookup function for {@link JdbcDynamicTableSource}.
 */
@Internal
public class JdbcRowDataLookupConnFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataLookupConnFunction.class);
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
    private transient DataSource dataSource;
    private transient Cache<RowData, List<RowData>> cache;
    private transient List<Connection> list;

    public JdbcRowDataLookupConnFunction(
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
            this.cache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
            list = new ArrayList<>();
            createDataSource();
        } catch (Exception sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
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
                for (RowData cachedRow : cachedRows) {
                    collect(cachedRow);
                }
                return;
            }
        }

        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            Connection connection = null;
            PreparedStatement preparedStatement = null;
            try {
                connection = this.createConnection();
                if (connection == null)
                    throw new RuntimeException("connection is null");
                    preparedStatement = connection.prepareStatement(query);
                if (preparedStatement == null)
                    return;
                preparedStatement.setQueryTimeout(600);
                //preparedStatement.clearParameters();
                preparedStatement = lookupKeyRowConverter.toExternal(keyRow, preparedStatement);
                ResultSet resultSet = null;
                if (preparedStatement != null && !preparedStatement.isClosed()) {
                    resultSet = preparedStatement.executeQuery();

                }

                if (cache == null) {
                    while (resultSet.next()) {
                        RowData row = jdbcRowConverter.toInternal(resultSet);
                        if (row != null)
                            collect(row);
                    }
                } else {
                    ArrayList<RowData> rows = new ArrayList<>();
                    while (resultSet.next()) {
                        RowData row = jdbcRowConverter.toInternal(resultSet);
                        if (row == null)
                            continue;
                        rows.add(row);
                        collect(row);
                    }
                    rows.trimToSize();
                    cache.put(keyRow, rows);
                }

                break;
           /* } catch (SQLException e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }
                try {
                    Thread.sleep(1000 * retry);

                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }


            } catch (InterruptedException e) {
                LOG.error("Execution of JDBC statement failed", e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException(e);
                }*/
            } catch (Exception e) {
                LOG.error("Execution of JDBC statement failed", e);
                LOG.error("Execution sql:"+query);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException(e);
                }
            } finally {
                try {
                    if (preparedStatement != null && !preparedStatement.isClosed())
                        preparedStatement.close();
                    if (connection != null && !connection.isClosed())
                        connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    private void createDataSource() throws Exception {
        //???????????????
        Properties properties = new Properties();
        properties.setProperty("driverClassName", drivername);
        properties.setProperty("url", dbURL);
        if (username != null) {
            properties.setProperty("username", username);
            properties.setProperty("password", password);
        }
        properties.setProperty("maxActive", String.valueOf(30));
        properties.setProperty("initialSize", String.valueOf(10));
        properties.setProperty("minIdle", String.valueOf(5));
        properties.setProperty("timeBetweenEvictionRunsMillis", String.valueOf(60000));
        properties.setProperty("minEvictableIdleTimeMillis", String.valueOf(300000));


        properties.setProperty("maxWait", String.valueOf(Duration.ofMinutes(10).toMillis()));
        properties.setProperty("poolPreparedStatements", String.valueOf(true));
        properties.setProperty("maxOpenPreparedStatements", String.valueOf(20));
        //???????????????
        this.dataSource = DruidDataSourceFactory.createDataSource(properties);
        ;


    }

    private Connection createConnection() throws Exception {
        if (this.dataSource != null) {
            try {
                Connection connection = this.dataSource.getConnection();
                if (connection == null || connection.isClosed()) {
                    connection = this.dataSource.getConnection();
                }
                return connection;
            } catch (NullPointerException exception) {
                this.createDataSource();

            } catch (SQLException exception) {
                this.createDataSource();

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return null;

    }

    private PreparedStatement getStatement() throws Exception {

        Connection connection = this.createConnection();
        int retry = 1;
        while (connection == null || connection.isClosed()) {
            if (retry >= 3)
                break;
            LOG.warn("JDBC connection IS NULL");
            Thread.sleep(1000);
            connection = this.createConnection();
            retry++;
        }
        if (connection != null)
            return connection.prepareStatement(query);

        else
            return null;
    }

    private Connection establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Class.forName(drivername);
        Connection connection;
        //DriverManager.setLoginTimeout(10 * 60);
        if (username == null) {
            connection = DriverManager.getConnection(dbURL);
        } else {
            connection = DriverManager.getConnection(dbURL, username, password);
        }
        return connection;
    }

    @Override
    public void close() throws IOException {
        if (cache != null) {
            cache.cleanUp();
            cache = null;
        }
    }


}
