package com.github.housepower.jdbc;

import com.github.housepower.jdbc.wrapper.SQLWrapper;
import com.github.housepower.log.Logger;
import com.github.housepower.log.LoggerFactory;
import com.github.housepower.misc.Validate;

import javax.sql.DataSource;
import javax.xml.bind.ValidationEvent;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author Kled
 * @date 2021/12/24 5:03 PM
 */
public class ClickhouseBalancedDataSource extends ClickhouseCommonDataSource implements DataSource {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseBalancedDataSource.class);

    private static final ScheduledExecutorService nodeChecker = Executors.newScheduledThreadPool(1);

    private int clusterIndex;

    private List<ClickhouseHikariDataSource> dataSources;

    {
        nodeChecker.schedule(new Runnable() {
            @Override
            public void run() {
                logger.info("cluster:" + clusterIndex + " node heartbeat check");
                dataSources.forEach(ClickhouseHikariDataSource::ping);
            }
        }, 3, TimeUnit.MINUTES);
    }

    public ClickhouseBalancedDataSource(int clusterIndex, List<Properties> properties) {
        this.clusterIndex = clusterIndex;
        dataSources = properties.stream().map(ClickhouseHikariDataSource::new).collect(Collectors.toList());
    }

    @Override
    public Connection getConnection() throws SQLException {
        ClickhouseHikariDataSource clickhouseHikariDataSource = dataSources.stream().filter(ClickhouseHikariDataSource::isAlive).findAny().orElse(null);
        Validate.ensure(clickhouseHikariDataSource != null, "cluster:" + clusterIndex + " has no available node");
        return clickhouseHikariDataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        ClickhouseHikariDataSource clickhouseHikariDataSource = dataSources.stream().filter(ClickhouseHikariDataSource::isAlive).findAny().orElse(null);
        Validate.ensure(clickhouseHikariDataSource != null, "cluster:" + clusterIndex + " has no available node");
        return clickhouseHikariDataSource.getConnection(username, password);
    }
}
