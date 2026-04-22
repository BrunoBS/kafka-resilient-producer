package com.example.kafka.service;

import com.example.kafka.model.LocalBuffer;
import com.example.kafka.repository.LocalBufferRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class LocalBufferService {

    private final LocalBufferRepository repository;
    private final KafkaProducerRegistry registry;

    public LocalBufferService(LocalBufferRepository repository, KafkaProducerRegistry registry) {
        this.repository = repository;
        this.registry = registry;
    }


    @Transactional
    public void save(String env, String topic, String key, String message, String error) {
        LocalBuffer buffer = new LocalBuffer();
        buffer.setEnvironment(env);
        buffer.setTopic(topic);
        buffer.setMessage(message);
        buffer.setKey(key);
        buffer.setLastError(error);
        buffer.setRetryCount(0);
        buffer.setStatus("PENDING");
        buffer.setNextRetry(LocalDateTime.now().plusSeconds(30));

        repository.save(buffer);
        System.out.println("Mensagem salva no buffer local para o ambiente: " + env);
    }


    @Transactional
    public void delete(Long id) {
        repository.deleteById(id);
    }


    @Transactional
    public void resetErrors(String env) {
        registry.reloadProducer(env);
        repository.resetStatusByEnv(env, "PENDING", 0, LocalDateTime.now());
    }


}
