package com.example.kafka.service;

import com.example.kafka.model.KafkaClusterProperty;
import com.example.kafka.model.KafkaTopicConfig;
import com.example.kafka.repository.KafkaClusterPropertyRepository;
import com.example.kafka.repository.KafkaTopicConfigRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class KafkaConfigurationService {
    private static final Logger log = LoggerFactory.getLogger(KafkaConfigurationService.class);

    private final KafkaClusterPropertyRepository clusterRepository;
    private final KafkaTopicConfigRepository topicRepository;
    private final KafkaProducerRegistry registry;

    public KafkaConfigurationService(
            KafkaClusterPropertyRepository clusterRepository,
            KafkaTopicConfigRepository topicRepository,
            KafkaProducerRegistry registry) {

        this.clusterRepository = clusterRepository;
        this.topicRepository = topicRepository;
        this.registry = registry;
    }

    @Transactional
    public KafkaClusterProperty saveProperties(KafkaClusterProperty request) {
        log.info("[KAFKA-CONFIG] Salvando configuração do ambiente {}.", request.getEnvironment());
        KafkaClusterProperty entity = clusterRepository.findByEnvironment(request.getEnvironment()).orElseGet(KafkaClusterProperty::new);
        entity.setEnvironment(request.getEnvironment());
        entity.setProperties(request.getProperties());
        KafkaClusterProperty saved = clusterRepository.save(entity);
        registry.reload(request.getEnvironment());

        log.info("[KAFKA-CONFIG] Configuração do ambiente {} salva com sucesso.", request.getEnvironment());
        return saved;
    }


    @Transactional(readOnly = true)
    public List<KafkaClusterProperty> findAllProperties() {
        return clusterRepository.findAll();
    }


    @Transactional(readOnly = true)
    public Optional<KafkaClusterProperty> findProperties(String environment) {
        return clusterRepository.findByEnvironment(environment);
    }

    @Transactional
    public boolean deleteProperties(String uuid) {
        Optional<KafkaClusterProperty> optional = clusterRepository.findById(uuid);
        if (optional.isEmpty()) {
            return false;
        }
        KafkaClusterProperty entity = optional.get();
        String environment = entity.getEnvironment();
        log.warn("[KAFKA-CONFIG] Removendo configuração do ambiente {}.", environment);
        clusterRepository.delete(entity);
        registry.reload(environment);
        log.info("[KAFKA-CONFIG] Configuração do ambiente {} removida.", environment);
        return true;
    }


    @Transactional
    public KafkaTopicConfig saveTopic(KafkaTopicConfig request) {

        log.info("[KAFKA-CONFIG] Salvando configuração do tópico {} no ambiente {}.",
                request.getTopic(),
                request.getEnvironment()
        );

        KafkaTopicConfig entity = topicRepository.findByEnvironmentAndTopic(request.getEnvironment(), request.getTopic())
                .stream()
                .findFirst()
                .orElseGet(KafkaTopicConfig::new);

        entity.setEnvironment(request.getEnvironment());
        entity.setTopic(request.getTopic());
        entity.setParametros(request.getParametros());
        KafkaTopicConfig saved = topicRepository.save(entity);
        log.info("[KAFKA-CONFIG] Configuração do tópico {} salva com sucesso.", request.getTopic());
        return saved;
    }

    @Transactional(readOnly = true)
    public List<KafkaTopicConfig> findAllTopics() {
        return topicRepository.findAll();
    }

    @Transactional(readOnly = true)
    public List<KafkaTopicConfig> findTopics(String environment, String name) {

        if (name == null || name.isBlank()) {
            return topicRepository.findByEnvironment(environment);
        }
        return topicRepository.findByEnvironmentAndTopic(environment, name);
    }

    @Transactional
    public boolean deleteTopic(String uuid) {

        Optional<KafkaTopicConfig> optional = topicRepository.findById(uuid);
        if (optional.isEmpty()) {
            return false;
        }

        KafkaTopicConfig entity = optional.get();
        log.warn("[KAFKA-CONFIG] Removendo configuração do tópico {} do ambiente {}.",
                entity.getTopic(),
                entity.getEnvironment()
        );
        topicRepository.delete(entity);
        return true;
    }
}
