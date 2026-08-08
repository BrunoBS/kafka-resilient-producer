package com.example.kafka.service;

import com.example.kafka.model.LocalBuffer;
import com.example.kafka.repository.KafkaClusterPropertyRepository;
import com.example.kafka.repository.KafkaTopicConfigRepository;
import com.example.kafka.repository.LocalBufferRepository;
import com.fasterxml.jackson.databind.JsonNode;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class KafkaRetryScheduler {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaRetryScheduler.class);

    private static final int BATCH_SIZE = 100;
    private static final int DEFAULT_MAX_RETRY = 5;
    private static final int INITIAL_BACKOFF_SECONDS = 120;
    private static final int MAX_BACKOFF_SECONDS = 3600;

    private final LocalBufferRepository repository;
    private final LocalBufferService localBufferService;
    private final KafkaMessagePublisher publisher;
    private final KafkaClusterPropertyRepository kafkaClusterPropertyRepository;
    private final KafkaTopicConfigRepository kafkaTopicConfigRepository;

    public KafkaRetryScheduler(
            LocalBufferRepository repository,
            LocalBufferService localBufferService,
            KafkaMessagePublisher publisher,
            KafkaClusterPropertyRepository kafkaClusterPropertyRepository,
            KafkaTopicConfigRepository kafkaTopicConfigRepository) {

        this.repository = repository;
        this.localBufferService = localBufferService;
        this.publisher = publisher;
        this.kafkaClusterPropertyRepository =
                kafkaClusterPropertyRepository;
        this.kafkaTopicConfigRepository =
                kafkaTopicConfigRepository;
    }


    @Scheduled(cron = "0 0 5 * * *")
    @SchedulerLock(name = "kafka-cleanup", lockAtMostFor = "PT10M")
    @Transactional
    public void purgeOldMessages() {

        log.info("[CLEANUP] Iniciando limpeza de mensagens antigas...");

        LocalDateTime thresholdError = LocalDateTime.now().minusDays(3);
        repository.deleteExpiredErrors("ERROR", thresholdError);

        LocalDateTime thresholdOld = LocalDateTime.now().minusDays(7);
        repository.deleteVeryOld(thresholdOld);

        log.info("[CLEANUP] Limpeza concluída."
        );
    }


    @Scheduled(cron = "0 * * * * *")
    @SchedulerLock(
            name = "kafka-retry-scheduler",
            lockAtMostFor = "PT5M"
    )
    public void processInStrictOrder() {

        long start = System.currentTimeMillis();
        log.info("[SCHEDULER] Iniciando ciclo de retry.");
        Map<String, Integer> maxRetryCache = new HashMap<>();
        try {
            List<String> environments = kafkaClusterPropertyRepository.findAllEnvironments();
            for (String environment : environments) {
                processEnvironment(environment, maxRetryCache);
            }
        } catch (Exception e) {
            log.error("[SCHEDULER-ERROR] Erro no ciclo de retry: {}", e.getMessage(), e);

        } finally {
            log.info("[SCHEDULER] Ciclo finalizado em {} ms. Configurações em cache={}",
                    System.currentTimeMillis() - start,
                    maxRetryCache.size()
            );
        }
    }

    private void processEnvironment(String environment, Map<String, Integer> maxRetryCache) {

        if (!publisher.isHealthy(environment)) {
            log.warn("[SCHEDULER] Ambiente {} está offline.", environment);
            return;
        }
        repository.fastTrackByEnv(environment, LocalDateTime.now());
        processEnvBatch(environment, maxRetryCache);
    }

    private void processEnvBatch(String environment, Map<String, Integer> maxRetryCache) {

        while (true) {
            List<LocalBuffer> batch = repository.findOldestPending(
                    environment,
                    LocalDateTime.now(),
                    PageRequest.of(0, BATCH_SIZE)
            );

            if (batch.isEmpty()) {
                return;
            }

            log.info("[SCHEDULER] Processando {} mensagens do ambiente {}.", batch.size(), environment);

            for (LocalBuffer message : batch) {
                try {
                    publishMessage(message);
                } catch (Exception e) {
                    log.error("[RETRY-FAILURE] Falha na mensagem {}: {}", message.getId(), e.getMessage());
                    int maxRetry = fetchMaxRetry(message, maxRetryCache);
                    handleFailure(message, e.getMessage(), maxRetry);
                    return;
                }
            }

            if (batch.size() < BATCH_SIZE) {
                return;
            }
        }
    }

    private void publishMessage(LocalBuffer message) throws Exception {

        RecordMetadata metadata = publisher.publish(
                message.getEventId(),
                message.getEnvironment(),
                message.getTopic(),
                message.getKey(),
                message.getMessage()
        );


        log.info("[KAFKA-SUCCESS] eventId={}, topic={}, partition={}, offset={}, key={},  environment={}",
                message.getEventId(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                message.getKey(),
                message.getEnvironment()
        );

        localBufferService.delete(message.getId());
    }


    private int fetchMaxRetry(LocalBuffer message, Map<String, Integer> cache) {
        String cacheKey = message.getEnvironment().concat("|").concat(message.getTopic());
        if (cache.containsKey(cacheKey)) {
            return cache.get(cacheKey);
        }
        int maxRetry = loadMaxRetryFromDatabase(message);
        cache.put(cacheKey, maxRetry);
        return maxRetry;
    }

    private int loadMaxRetryFromDatabase(
            LocalBuffer message) {

        return kafkaTopicConfigRepository
                .findByEnvironmentAndTopic(
                        message.getEnvironment(),
                        message.getTopic()
                )
                .stream()
                .findFirst()
                .map(config -> {

                    JsonNode params =
                            config.getParametros();

                    if (params != null
                            && params.has(
                            "max.retry.attempts")) {

                        return params
                                .get("max.retry.attempts")
                                .asInt();
                    }

                    return DEFAULT_MAX_RETRY;
                })
                .orElse(DEFAULT_MAX_RETRY);
    }


    private void handleFailure(LocalBuffer message, String error, int maxRetry) {
        int nextAttempt = message.getRetryCount() + 1;
        message.setRetryCount(nextAttempt);
        message.setLastError(error);
        long secondsToWait = (long) (INITIAL_BACKOFF_SECONDS * Math.pow(2, nextAttempt - 1));
        secondsToWait = Math.min(secondsToWait, MAX_BACKOFF_SECONDS);
        message.setNextRetry(LocalDateTime.now().plusSeconds(secondsToWait));

        if (nextAttempt >= maxRetry) {
            message.setStatus("ERROR");
        } else {
            message.setStatus("FAILED_RETRY");
        }

        repository.save(message);
        log.warn(
                "[BACKOFF] id={} eventId={} attempt={}/{} status={} nextRetry={} ",
                message.getId(),
                message.getEventId(),
                nextAttempt,
                maxRetry,
                message.getStatus(),
                message.getNextRetry()
        );
    }
}