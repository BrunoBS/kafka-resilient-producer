package com.example.kafka.service;

import com.example.kafka.repository.KafkaClusterPropertyRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;


@Component
public class KafkaProducerRegistry {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerRegistry.class);
    private final Map<String, KafkaProducer<String, String>> producers = new ConcurrentHashMap<>();
    private final KafkaClusterPropertyRepository configRepository;
    private final Executor kafkaPublishExecutor;
    private final ObjectMapper objectMapper;

    public KafkaProducerRegistry(KafkaClusterPropertyRepository configRepository, Executor kafkaPublishExecutor, ObjectMapper objectMapper) {
        this.configRepository = configRepository;
        this.kafkaPublishExecutor = kafkaPublishExecutor;
        this.objectMapper = objectMapper;
    }

    public KafkaProducer<String, String> get(String env) {
        return producers.computeIfAbsent(env, this::createNewProducer);
    }

    private KafkaProducer<String, String> createNewProducer(String env) {
        Map<String, Object> props = loadConfiguration(env);
        Object bootstrapServers = props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (bootstrapServers == null || bootstrapServers.toString().isBlank()) {
            log.error("[KAFKA-REGISTRY] Erro ao criar Producer: '{}' não configurado para o ambiente: {}",
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env);
            throw new IllegalArgumentException("Configuração de bootstrap.servers ausente para: " + env);
        }

        // Define timeout de 3s para evitar que o Scheduler trave se o Kafka estiver offline
        props.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        log.info("[KAFKA-REGISTRY] Criando novo Producer para o ambiente: {} em: {}",
                env, props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

        try {
            return new KafkaProducer<>(props);
        } catch (Exception e) {
            log.error("[KAFKA-REGISTRY] Falha crítica ao instanciar KafkaProducer para {}: {}", env, e.getMessage());
            throw e;
        }
    }

    public Map<String, Object> loadConfiguration(String environment) {
        return configRepository.findByEnvironment(environment)
                .map(config -> objectMapper.convertValue(config.getProperties(),
                        new TypeReference<Map<String, Object>>() {
                        }))
                .orElseGet(() -> {
                    log.warn("[KAFKA-REGISTRY] Nenhuma configuração encontrada no banco para o ambiente: {}", environment);
                    return new HashMap<>();
                });
    }

    public void reloadProducer(String env) {
        log.info("[KAFKA-REGISTRY] Solicitando recarregamento do produtor para o ambiente: {}", env);
        KafkaProducer<String, String> oldProducer = producers.remove(env);
        if (oldProducer != null) {
            CompletableFuture.runAsync(() -> {
                        try {
                            log.debug("[KAFKA-REGISTRY] Fechando produtor antigo de {}...", env);
                            oldProducer.close(Duration.ofSeconds(5));
                        } catch (Exception e) {
                            log.error("[KAFKA-REGISTRY] Erro ao fechar produtor de {}: {}", env, e.getMessage());
                        }
                    }, kafkaPublishExecutor)
                    .orTimeout(10, TimeUnit.SECONDS);
        }
    }


    public boolean isHealthy(String env) {
        try {
            KafkaProducer<String, String> p = this.get(env);
            p.partitionsFor("health-check");
            return true;
        } catch (Exception e) {
            log.warn("[KAFKA-OFFLINE] Ambiente {} inacessível. Abortando conexões em background...", env);
            this.reloadProducer(env);
            return false;
        }
    }


    @PreDestroy
    public void shutdownAll() {
        log.info("[KAFKA-REGISTRY] Encerrando todos os Kafka Producers ativos (PreDestroy)...");
        producers.forEach((env, producer) -> {
            try {
                producer.close(Duration.ofSeconds(2));
                log.info("[KAFKA-REGISTRY] Producer {} encerrado com sucesso.", env);
            } catch (Exception e) {
                log.error("[KAFKA-REGISTRY] Erro ao fechar {} no shutdown: {}", env, e.getMessage());
            }
        });
        producers.clear();
    }
}
