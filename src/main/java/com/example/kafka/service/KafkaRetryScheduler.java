package com.example.kafka.service;

import com.example.kafka.model.LocalBuffer;
import com.example.kafka.repository.KafkaClusterPropertyRepository;
import com.example.kafka.repository.KafkaTopicConfigRepository;
import com.example.kafka.repository.LocalBufferRepository;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.transaction.Transactional;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class KafkaRetryScheduler {
    private static final Logger log = LoggerFactory.getLogger(KafkaRetryScheduler.class);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final LocalBufferRepository repository;
    private final KafkaClusterPropertyRepository kafkaClusterPropertyRepository;
    private final KafkaTopicConfigRepository kafkaTopicConfigRepository;
    private final KafkaProducerRegistry registry;
    private static final int BATCH_SIZE = 100;

    public KafkaRetryScheduler(LocalBufferRepository repository,
                               KafkaClusterPropertyRepository kafkaClusterPropertyRepository,
                               KafkaTopicConfigRepository kafkaTopicConfigRepository,
                               KafkaProducerRegistry registry) {
        this.repository = repository;
        this.kafkaClusterPropertyRepository = kafkaClusterPropertyRepository;
        this.kafkaTopicConfigRepository = kafkaTopicConfigRepository;
        this.registry = registry;
    }

    @Scheduled(cron = "0 0 5 * * *")
    @Transactional
    public void purgeOldMessages() {
        log.info("[CLEANUP] Iniciando limpeza de mensagens mortas...");
        LocalDateTime thresholdError = LocalDateTime.now().minusDays(3);
        repository.deleteExpiredErrors("ERROR", thresholdError);
        LocalDateTime thresholdOld = LocalDateTime.now().minusDays(7);
        repository.deleteVeryOld(thresholdOld);
        log.info("[CLEANUP] Concluído.");
    }

    // Executa a cada minuto no segudo 0
    @Scheduled(cron = "0 * * * * *")
    public void processInStrictOrder() {
        if (!isRunning.compareAndSet(false, true)) return;
        try {
            log.info("[SCHEDULER] Iniciando ciclo de reprocessamento autônomo...");
            List<String> environments = kafkaClusterPropertyRepository.findAllEnvironments();
            for (String env : environments) {

                if (registry.isHealthy(env)) {
                    repository.fastTrackByEnv(env, LocalDateTime.now());
                    processEnvBatch(env);
                } else {
                    log.warn("[SCHEDULER-NOT-HEALTHY] Ambiente {} ainda offline. Pulando para o próximo ambiente.", env);
                }
            }
        } catch (Exception e) {
            log.error("[SCHEDULER-ERROR-CRITICAL] Erro no ciclo do Scheduler: {}", e.getMessage(), e);
        } finally {
            isRunning.set(false);
        }
    }

    private void processEnvBatch(String environment) {
        log.info("[OO] Processando ambiente: {}", environment);
        boolean hasMore = true;
        while (hasMore) {
            List<LocalBuffer> batch = repository.findOldestPending(environment, LocalDateTime.now(), PageRequest.of(0, BATCH_SIZE));
            if (batch.isEmpty()) {
                hasMore = false;
                continue;
            }

            for (LocalBuffer message : batch) {
                try {
                    var producer = registry.get(environment);
                    RecordMetadata metadata = producer
                            .send(new ProducerRecord<>(message.getTopic(), message.getKey(), message.getMessage()))
                            .get(3, TimeUnit.SECONDS);
                    log.info("[KAFKA-SUCCESS] Mensagem salva no tópico {} partição {} offset {}",
                            metadata.topic(), metadata.partition(), metadata.offset());

                    repository.delete(message);

                } catch (Exception e) {
                    log.error("[BATCH-FAILURE] Falha ao reenviar mensagem {}: {}", message.getId(), e.getMessage());
                    handleFailure(message, e.getMessage(), fetchMaxRetry(message));
                    hasMore = false;
                    break;
                }
            }
            if (batch.size() < BATCH_SIZE) hasMore = false;
        }
    }

    private int fetchMaxRetry(LocalBuffer message) {
        return kafkaTopicConfigRepository.findByEnvironmentAndTopic(message.getEnvironment(), message.getTopic())
                .stream().findFirst()
                .map(config -> {
                    JsonNode params = config.getParametros();
                    return (params != null && params.has("max.retry.attempts")) ? params.get("max.retry.attempts").asInt() : 5;
                }).orElse(5);
    }

    private void handleFailure(LocalBuffer b, String err, int max) {
        int nextAttempt = b.getRetryCount() + 1;
        b.setRetryCount(nextAttempt);
        b.setLastError(err);


        long secondsToWait = (long) (120 * Math.pow(2, nextAttempt - 1));
        secondsToWait = Math.min(secondsToWait, 3600); // Máximo 1 hora

        b.setNextRetry(LocalDateTime.now().plusSeconds(secondsToWait));
        b.setStatus(nextAttempt >= max ? "ERROR" : "FAILED_RETRY");

        repository.save(b);
        System.out.printf("[BACKOFF] Mensagem %d falhou (%d/%d). Próxima tentativa em %d segundos.%n",
                b.getId(), nextAttempt, max, secondsToWait);

        log.warn("[BACKOFF] Mensagem {} agendada para daqui a {}s.", b.getId(), secondsToWait);
    }
}
