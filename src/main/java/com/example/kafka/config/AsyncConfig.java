package com.example.kafka.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean(name = "kafkaPublishExecutor")
    public Executor kafkaPublishExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(40); // Garante 40 threads sempre prontas
        executor.setMaxPoolSize(100); // Pode subir até 100 se o banco/kafka travar
        executor.setQueueCapacity(50); // Fila pequena para "falhar rápido" e ir pro banco
        executor.setThreadNamePrefix("Kafka-Async-");

        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }

}
