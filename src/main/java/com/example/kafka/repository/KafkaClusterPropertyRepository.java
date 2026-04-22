package com.example.kafka.repository;

import com.example.kafka.model.KafkaClusterProperty;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.Optional;

public interface KafkaClusterPropertyRepository extends JpaRepository<KafkaClusterProperty, String> {

    @Cacheable(value = "clusterConfigs", key = "#environment")
    Optional<KafkaClusterProperty> findByEnvironment(String environment);

    @Cacheable(value = "clusterConfigs", key = "'all-envs'")
    @Query("SELECT DISTINCT p.environment FROM KafkaClusterProperty p")
    List<String> findAllEnvironments();

    @Override
    @Caching(evict = {
            // Limpa a config específica do ambiente no Redis
            @CacheEvict(value = "clusterConfigs", key = "#entity.environment"),
            // Limpa a lista de nomes de ambientes para refletir novos cadastros
            @CacheEvict(value = "clusterConfigs", key = "'all-envs'")
    })
    <S extends KafkaClusterProperty> S save(S entity);

}