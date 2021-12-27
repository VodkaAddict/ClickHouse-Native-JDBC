package com.github.housepower.jdbc;

import com.github.housepower.exception.InvalidValueException;
import com.github.housepower.log.Logger;
import com.github.housepower.log.LoggerFactory;
import com.github.housepower.misc.Validate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Kled
 * @date 2021/12/24 5:03 PM
 */
public class ClickhouseClusterDataSource extends ClickhouseCommonDataSource implements DataSource {

    private static final Logger logger = LoggerFactory.getLogger(ClickhouseClusterDataSource.class);

    private static final Pattern URL_TEMPLATE = Pattern.compile(ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX +
            "//([a-zA-Z0-9_:;,.-]+)" +
            "((/[a-zA-Z0-9_]+)?" +
            "([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]*)*)?" +
            ")?");

    private static final ScheduledExecutorService clusterSwitcher = Executors.newScheduledThreadPool(1);

    private List<ClickhouseBalancedDataSource> clusters = new ArrayList<>();

    private int activeClusterIndex = 0;

    {
        clusterSwitcher.schedule(new Runnable() {
            @Override
            public void run() {
                logger.info("cluster switcher is running");
                //集群负载，5分钟内访问同一个集群
                if (activeClusterIndex == clusters.size() - 1) {
                    activeClusterIndex = 0;
                } else {
                    activeClusterIndex++;
                }
            }
        }, 5, TimeUnit.MINUTES);
    }

    public ClickhouseClusterDataSource(String url, Properties properties) {
        Matcher m = URL_TEMPLATE.matcher(url);
        Validate.ensure(m.matches(), "Incorrect url: " + url);

        String clusterIpStr = m.group(1);
        String dbParams = m.group(2);
        String[] clusterIps = clusterIpStr.split(";");
        for (int i = 0; i < clusterIps.length; i++) {
            final int clusterIndex = i;
            List<Properties> propertiesList = Arrays.stream(clusterIps[i].split(",")).map(host -> {
                Properties propertiesCopy = new Properties(properties);
                propertiesCopy.setProperty("jdbcUrl", ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX + "//" + host + dbParams);
                propertiesCopy.setProperty("driverClassName", "com.github.housepower.jdbc.ClickHouseDriver");
                propertiesCopy.setProperty("poolName", "cluster-index:" + clusterIndex + ",host:" + host);
                return propertiesCopy;
            }).collect(Collectors.toList());
            clusters.add(new ClickhouseBalancedDataSource(clusterIndex, propertiesList));
        }

        if (clusters.isEmpty()) {
            throw new InvalidValueException("clickhouse cluster node is empty");
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return clusters.get(activeClusterIndex).getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return clusters.get(activeClusterIndex).getConnection(username, password);
    }

    public static void main(String[] args) {
        Pattern URL_TEMPLATE = Pattern.compile(ClickhouseJdbcUrlParser.JDBC_CLICKHOUSE_PREFIX +
                "//([a-zA-Z0-9_:;,.-]+)" +
                "((/[a-zA-Z0-9_]+)?" +
                "([?][a-zA-Z0-9_]+[=][a-zA-Z0-9_]+([&][a-zA-Z0-9_]+[=][a-zA-Z0-9_]*)*)?" +
                ")?");
        String url = "jdbc:clickhouse://123.12.3.1:8888,123.12.3.2:8889;123.12.4.1:8888,123.12.4.2:8889/test_db?useSSL=false&autoReconnect=true&useUnicode=true&characterEncoding=UTF8&zeroDateTimeBehavior=convertToNull&allowMultiQueries=true";
        Matcher m = URL_TEMPLATE.matcher(url);
        System.out.println(m.matches());
        System.out.println(m.group(1));
        System.out.println(m.group(2));
    }
}
