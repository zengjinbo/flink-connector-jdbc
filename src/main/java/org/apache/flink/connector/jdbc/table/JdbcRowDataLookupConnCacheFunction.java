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

import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.dialect.JdbcDialects;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.options.JdbcLookupOptions;
import org.apache.flink.connector.jdbc.internal.options.JdbcOptions;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A lookup function for {@link JdbcDynamicTableSource}.
 */
@Internal
public class JdbcRowDataLookupConnCacheFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcRowDataLookupConnCacheFunction.class);
    private static final long serialVersionUID = 1L;

    private final String query;
    private final String queryWhere;
    private final String drivername;
    private final String dbURL;
    private final String username;
    private final String password;
    private final DataType[] keyTypes;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final String[] keyNames;
    private final int maxRetryTimes;
    private final JdbcDialect jdbcDialect;
    private final JdbcRowConverter jdbcRowConverter;
    private final JdbcRowConverter lookupKeyRowConverter;
    private transient DataSource dataSource;
    private transient Cache<RowData, RowData> cache;
    private transient Cache<RowData, RowData> newcache;
    private transient Cache<RowData, Boolean> keyCache;
    private transient ScheduledExecutorService executor;
    private transient ScheduledFuture scheduledFuture;
    private transient List<Connection> list;

    public JdbcRowDataLookupConnCacheFunction(
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
                options.getTableName(), fieldNames, null);
        this.queryWhere = options.getDialect().getSelectFromStatement(
                options.getTableName(), fieldNames, keyNames);
        this.keyNames=keyNames;
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
            this.newcache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
            this.keyCache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                    .maximumSize(cacheMaxSize)
                    .build();
            this.executor = Executors.newScheduledThreadPool(
                    2, new ExecutorThreadFactory("jdbc-lookup-upsert-sink-flusher"));
            this.scheduledFuture = this.executor.scheduleWithFixedDelay(() -> {

                try {
                    getCache();
                } catch (Exception e) {
                    // fail the sink and skip the rest of the items
                    // if the failure handler decides to throw an exception
                }
            }, cacheExpireMs, cacheExpireMs, TimeUnit.MILLISECONDS);
            executor.schedule(()->{getCache();},1,TimeUnit.SECONDS);
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
            RowData cachedRows = cache.getIfPresent(keyRow);
            if (cachedRows != null) {

                    collect(cachedRows);

                return;
            }
            else if (keyCache != null && keyCache.getIfPresent(keyRow)==null) {
                for (int retry = 1; retry <= maxRetryTimes; retry++) {
                    PreparedStatement preparedStatement = null;
                    try {
                        preparedStatement = this.getStatement();
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
                            while (resultSet.next()) {
                                RowData row = jdbcRowConverter.toInternal(resultSet);
                                if (row == null)
                                    continue;
                                collect(row);
                                cache.put(keyRow, row);
                            }


                        }

                        break;
                    } catch (SQLException e) {
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
                        }
                    } catch (Exception e) {
                        LOG.error("Execution of JDBC statement failed", e);
                        if (retry >= maxRetryTimes) {
                            throw new RuntimeException(e);
                        }
                    } finally {
                        try {
                            if (preparedStatement != null && !preparedStatement.isClosed())
                                preparedStatement.close();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                        keyCache.put(keyRow,true);
                    }
                }
            }
        }


    }

    private void createDataSource() throws Exception {
        //数据源配置
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
        //数据源配置
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
            return connection.prepareStatement(queryWhere);

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
    public void getCache() {
        this.newcache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                .maximumSize(cacheMaxSize)
                .build();
        for (int retry = 1; retry <= maxRetryTimes; retry++) {
            try {
                Connection connection = this.establishConnectionAndStatement();
                getCache(connection, null, null);
                if (connection != null && !connection.isClosed())
                    connection.close();
            } catch (Exception e) {
                LOG.error(String.format("JDBC executeBatch error, retry times = %d", retry), e);
                if (retry >= maxRetryTimes) {
                    throw new RuntimeException("Execution of JDBC statement failed.", e);
                }
                try {
                    Thread.sleep(1000 * retry);
                } catch (InterruptedException interruptedException) {
                    throw new RuntimeException("Execution of JDBC  failed.", interruptedException);

                }


            }
        }
        Cache<RowData, RowData> cache=this.cache;
        this.cache=this.newcache;
        cache.cleanUp();
        this.keyCache = cacheMaxSize == -1 || cacheExpireMs == -1 ? null : CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                .maximumSize(cacheMaxSize)
                .build();
    }
    public RowData getCache(Connection connection, String sql, Object[] keys) throws SQLException, ClassNotFoundException {
        RowData row = null;


        PreparedStatement statement = null;
        try {
            if (connection == null || connection.isClosed())
                connection = this.createConnection();
            if (StringUtils.isNotEmpty(sql)) {
                statement = connection.prepareStatement(sql);
                statement = lookupKeyRowConverter.toExternal(GenericRowData.of(keys), statement);
            } else {
                statement = connection.prepareStatement(query);
            }

            ResultSet resultSet = statement.executeQuery();
            if (newcache == null) {
                while (resultSet.next()) {
                    collect(jdbcRowConverter.toInternal(resultSet));

                }
            } else {
                while (resultSet.next()) {
                    row = getCache(resultSet);
                }
            }


        } catch (SQLException | ClassNotFoundException e) {

            throw new RuntimeException("Execution of JDBC statement failed.", e);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null && !statement.isClosed())
                    statement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
                throw new RuntimeException("Execution of JDBC statement failed.", throwables);
            }


        }
        return row;
    }
    public RowData getCache(ResultSet resultSet) throws SQLException {
        RowData row = null;
        while (resultSet.next()) {
            row = jdbcRowConverter.toInternal(resultSet);
            ;
            Object[] keys = convertTokeyRowFromResultSet(resultSet);
            newcache.put(GenericRowData.of(keys), row);
        }
        return row;

    }
    private Object[] convertTokeyRowFromResultSet(ResultSet resultSet) throws SQLException {
        ArrayList<Object> keys = new ArrayList();
        ResultSetMetaData m = resultSet.getMetaData();
        for (int i = 0; i < m.getColumnCount(); i++) {
            for (int y = 0; y < keyTypes.length; y++) {
                if (keyNames[y].equals(m.getColumnName(i + 1))) {
                    keys.add(resultSet.getObject(i + 1));
                }
            }
        }

        return keys.toArray();
    }
}


