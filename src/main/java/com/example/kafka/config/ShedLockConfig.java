package com.example.kafka.config;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider.ColumnNames;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

@Configuration
@EnableSchedulerLock(defaultLockAtMostFor = "PT5M")
public class ShedLockConfig {

    @Value("${server.port}")
    private String serverPort;

    private final String instanceId = UUID.randomUUID().toString();
    @Bean
    public LockProvider lockProvider(DataSource dataSource) {

        String lockedBy = getInstanceId();

        return new JdbcTemplateLockProvider(
                JdbcTemplateLockProvider.Configuration.builder()
                        .withJdbcTemplate(new JdbcTemplate(dataSource))
                        .withLockedByValue(lockedBy)
                        .withTableName("TB_SCHEDULER_LOCK")
                        .withColumnNames(
                                new ColumnNames(
                                        "NM_LOCK",
                                        "DT_LOCK_UNTIL",
                                        "DT_LOCKED_AT",
                                        "NM_LOCKED_BY"
                                )
                        )
                        .usingDbTime()
                        .build()
        );
    }

    private String getInstanceId() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            return hostname + ":" + serverPort;
        } catch (UnknownHostException e) {
            return "instance:" + instanceId + ":" + serverPort;
        }
    }
}