package com.example.kafka;

import com.example.kafka.service.KafkaRetryScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableAsync
@EnableCaching
@EnableRetry
@EnableJpaAuditing
public class KafkaDynamicApplication {
    private static final Logger log = LoggerFactory.getLogger(KafkaDynamicApplication.class);
    public static void main(String[] args) {
        SpringApplication.run(KafkaDynamicApplication.class, args);
        log.info("Iniciado o Kafka Dynamic Application...");

    }
}