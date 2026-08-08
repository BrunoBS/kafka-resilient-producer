package com.example.kafka.service;

import com.example.kafka.repository.KafkaClusterPropertyRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
public class KafkaProducerRegistry {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaProducerRegistry.class);

    private static final int MAX_BLOCK_MS = 3000;
    private static final int REQUEST_TIMEOUT_MS = 3000;
    private static final int DELIVERY_TIMEOUT_MS = 5000;
    private static final int HEALTH_CHECK_TIMEOUT_SECONDS = 3;


    private final Map<String, KafkaProducer<String, String>> producers = new ConcurrentHashMap<>();


    private final Map<String, AdminClient> adminClients = new ConcurrentHashMap<>();
    private final KafkaClusterPropertyRepository configRepository;
    private final ObjectMapper objectMapper;

    public KafkaProducerRegistry(KafkaClusterPropertyRepository configRepository, ObjectMapper objectMapper) {
        this.configRepository = configRepository;
        this.objectMapper = objectMapper;
    }


    public KafkaProducer<String, String> get(String environment) {
        return producers.computeIfAbsent(environment, this::createNewProducer);
    }

    private KafkaProducer<String, String> createNewProducer(String environment) {

        Map<String, Object> props = loadConfiguration(environment);
        validateBootstrapServers(environment, props);
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.putIfAbsent(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS);
        props.putIfAbsent(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        props.putIfAbsent(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DELIVERY_TIMEOUT_MS);
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
        props.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        log.info("[KAFKA-REGISTRY] Criando Producer para o ambiente {}.", environment);
        try {
            return new KafkaProducer<>(props);
        } catch (Exception e) {

            log.error("[KAFKA-REGISTRY] Falha ao criar Producer para {}: {}", environment, e.getMessage(), e);
            throw e;
        }
    }


    public boolean isHealthy(String environment) {
        try {
            AdminClient adminClient = getAdminClient(environment);
            adminClient.describeCluster().nodes().get(HEALTH_CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            log.debug("[KAFKA-HEALTH] Ambiente {} está saudável.", environment);
            return true;

        } catch (Exception e) {
            log.warn("[KAFKA-HEALTH] Ambiente {} está inacessível: {}", environment, e.getMessage());
            return false;
        }
    }

    private AdminClient getAdminClient(String environment) {
        return adminClients.computeIfAbsent(environment, this::createAdminClient);
    }

    private AdminClient createAdminClient(String environment) {
        Map<String, Object> props = loadConfiguration(environment);
        validateBootstrapServers(environment, props);
        log.info("[KAFKA-REGISTRY] Criando AdminClient para o ambiente {}.", environment);
        try {
            return AdminClient.create(props);
        } catch (Exception e) {
            log.error("[KAFKA-REGISTRY] Falha ao criar AdminClient para {}: {}", environment, e.getMessage(), e);
            throw e;
        }
    }

    public void reload(String environment) {
        log.info("[KAFKA-REGISTRY] Recarregando recursos do ambiente {}.", environment);
        reloadProducer(environment);
        reloadAdminClient(environment);
    }

    public void reloadProducer(String environment) {
        log.info("[KAFKA-REGISTRY] Recarregando Producer do ambiente {}.", environment);
        KafkaProducer<String, String> oldProducer = producers.remove(environment);
        if (oldProducer == null) {
            return;
        }
        try {
            oldProducer.close(Duration.ofSeconds(5));
            log.info("[KAFKA-REGISTRY] Producer antigo de {} fechado.", environment);
        } catch (Exception e) {
            log.error("[KAFKA-REGISTRY] Erro ao fechar Producer de {}: {}", environment, e.getMessage(), e);
        }
    }

    private void reloadAdminClient(String environment) {
        AdminClient adminClient = adminClients.remove(environment);
        if (adminClient == null) {
            return;
        }
        try {
            adminClient.close(Duration.ofSeconds(5));
            log.info("[KAFKA-REGISTRY] AdminClient antigo de {} fechado.", environment);
        } catch (Exception e) {
            log.error("[KAFKA-REGISTRY] Erro ao fechar AdminClient de {}: {}", environment, e.getMessage(), e);
        }
    }


    public Map<String, Object> loadConfiguration(String environment) {
        return configRepository.findByEnvironment(environment)
                .map(config ->
                        objectMapper.convertValue(config.getProperties(),
                                new TypeReference<Map<String, Object>>() {
                                }
                        )
                )
                .orElseGet(() -> {
                    log.warn("[KAFKA-REGISTRY] " + "Nenhuma configuração encontrada " + "para o ambiente {}.", environment);
                    return new HashMap<>();
                });
    }


    private void validateBootstrapServers(String environment, Map<String, Object> props) {
        Object bootstrapServers = props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        if (bootstrapServers == null || bootstrapServers.toString().isBlank()) {
            log.error("[KAFKA-REGISTRY] bootstrap.servers não configurado para o ambiente {}.", environment);
            throw new IllegalArgumentException("Configuração de bootstrap.servers ausente para: " + environment);
        }
    }


    @PreDestroy
    public void shutdownAll() {
        log.info("[KAFKA-REGISTRY] Encerrando Kafka Producers e AdminClients...");
        producers.forEach(
                (environment, producer) -> {
                    try {
                        producer.close(Duration.ofSeconds(2));
                        log.info("[KAFKA-REGISTRY Producer {} encerrado.", environment);

                    } catch (Exception e) {
                        log.error("[KAFKA-REGISTRY] Erro ao fechar Producer {}: {}", environment, e.getMessage(), e);
                    }
                }
        );

        adminClients.forEach(
                (environment, adminClient) -> {
                    try {
                        adminClient.close(Duration.ofSeconds(2));
                        log.info("[KAFKA-REGISTRY] AdminClient {} encerrado.", environment);
                    } catch (Exception e) {
                        log.error("[KAFKA-REGISTRY] Erro ao fechar AdminClient {}: {}", environment, e.getMessage(), e);
                    }
                }
        );
        producers.clear();
        adminClients.clear();
    }
}