package com.example.kafka.repository;

import com.example.kafka.model.KafkaTopicConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface KafkaTopicConfigRepository extends JpaRepository<KafkaTopicConfig, String> {

    @Cacheable(value = "topicConfigs", key = "#environment + '-' + #topic")
    List<KafkaTopicConfig> findByEnvironmentAndTopic(String environment, String topic);

    @Cacheable(value = "topicConfigs", key = "'list-' + #environment")
    List<KafkaTopicConfig> findByEnvironment(String environment);

    @Override
    @Caching(evict = {
            @CacheEvict(value = "topicConfigs", key = "#entity.environment + '-' + #entity.topic"),
            @CacheEvict(value = "topicConfigs", key = "'list-' + #entity.environment")
    })
    <S extends KafkaTopicConfig> S save(S entity);

    @Override
    @Caching(evict = {
            @CacheEvict(value = "topicConfigs", key = "#entity.environment + '-' + #entity.topic"),
            @CacheEvict(value = "topicConfigs", key = "'list-' + #entity.environment")
    })
    void delete(KafkaTopicConfig entity);

}