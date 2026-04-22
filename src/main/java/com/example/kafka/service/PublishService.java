package com.example.kafka.service;

import com.example.kafka.repository.KafkaTopicConfigRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class PublishService {
    private static final Logger log = LoggerFactory.getLogger(PublishService.class);
    private final KafkaProducerRegistry registry;
    private final LocalBufferService bufferService;
    private final KafkaTopicConfigRepository kafkaTopicConfigRepository;


    public PublishService(KafkaProducerRegistry registry,
                          LocalBufferService bufferService, KafkaTopicConfigRepository kafkaTopicConfigRepository) {
        this.registry = registry;
        this.bufferService = bufferService;
        this.kafkaTopicConfigRepository = kafkaTopicConfigRepository;
    }

    @Async("kafkaPublishExecutor")
    public void publish(String environment, String topic, String key, String message) {
        synchronized (environment.intern()) {
            log.info("[PREPARANDO ...] preparando para mandar mensagem no ambiente {} e topic  {}", environment, topic);
            try {
                var topicConfig = kafkaTopicConfigRepository.findByEnvironmentAndTopic(environment, topic);
                if (topicConfig.isEmpty()) {
                    log.warn("[REJEITADO] Tópico {} não cadastrado em {}.", topic, environment);
                    return;
                }

                var producer = registry.get(environment);
                if (producer == null) {
                    bufferService.save(environment, topic, key, message, "producer null");
                    return;
                }

                RecordMetadata metadata = producer
                        .send(new ProducerRecord<>(topic, key, message))
                        .get(3, TimeUnit.SECONDS);

                log.info("[KAFKA-SUCCESS] Mensagem salva no tópico {} partição {} offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());

            } catch (Exception e) {
                log.error("[FAIL-RECOVERY] Erro no envio direto ({}). Salvando no Buffer.", e.getMessage());
                bufferService.save(environment, topic, key, message, e.getMessage());
            }
        }
    }
}
