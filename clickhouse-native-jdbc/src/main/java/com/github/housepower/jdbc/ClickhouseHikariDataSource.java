package com.github.housepower.jdbc;

import com.github.housepower.exception.InvalidValueException;
import com.github.housepower.log.Logger;
import com.github.housepower.log.LoggerFactory;
import com.github.housepower.settings.ClickHouseConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;

/**
 * @author Kled
 * @date 2021/12/24 5:03 PM
 */
public class ClickhouseHikariDataSource extends HikariDataSource {

    private String jdbcUrl;
    private final ClickHouseConfig cfg;
    private final ClickHouseDriver driver = new ClickHouseDriver();
    private boolean alive = true;

    public ClickhouseHikariDataSource(Properties properties) {
        super(new HikariConfig(properties));

        this.jdbcUrl = (String) properties.get("jdbcUrl");
        this.cfg = ClickHouseConfig.Builder.builder()
                .withJdbcUrl(jdbcUrl)
                .withSettings(ClickhouseJdbcUrlParser.parseProperties(properties))
                .host("undefined")
                .port(0)
                .build();
    }

    public boolean isAlive() {
        return alive;
    }

    public boolean ping() {
        boolean pong = true;
        try (ClickHouseConnection connection = driver.connect(jdbcUrl, cfg)) {
            pong = connection.ping(Duration.ofSeconds(1));
        } catch (Exception e) {
            pong = false;
        }
        alive = pong;
        return pong;
    }
}
