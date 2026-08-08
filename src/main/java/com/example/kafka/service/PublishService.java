package com.example.kafka.service;

import com.example.kafka.repository.KafkaTopicConfigRepository;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class PublishService {

    private static final Logger log = LoggerFactory.getLogger(PublishService.class);

    private final KafkaMessagePublisher publisher;
    private final LocalBufferService bufferService;
    private final KafkaTopicConfigRepository topicConfigRepository;

    public PublishService(
            KafkaMessagePublisher publisher,
            LocalBufferService bufferService,
            KafkaTopicConfigRepository topicConfigRepository) {

        this.publisher = publisher;
        this.bufferService = bufferService;
        this.topicConfigRepository = topicConfigRepository;
    }

    @Async("kafkaPublishExecutor")
    public void publish(
            String environment,
            String topic,
            String key,
            String message) {
        String eventId = UUID.randomUUID().toString();
        log.info("[PUBLISH] Iniciando publicação. eventId={} environment={} topic={}",
                eventId,
                environment,
                topic
        );

        try {
            if (!isTopicConfigured(environment, topic)) {
                log.warn("[REJECTED] Tópico não cadastrado. eventId={} environment={} topic={}",
                        eventId,
                        environment,
                        topic
                );
                return;
            }

            RecordMetadata metadata =
                    publisher.publish(
                            eventId,
                            environment,
                            topic,
                            key,
                            message
                    );


            log.info("[KAFKA-SUCCESS] eventId={}, topic={}, partition={}, offset={}, key={},  environment={}",
                    eventId,
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    key,
                    environment
            );

        } catch (Exception e) {
            log.error("[FAIL-RECOVERY] Falha na publicação. eventId={} environment={} topic={}. Mensagem será armazenada no LocalBuffer.",
                    eventId,
                    environment,
                    topic,
                    e
            );

            bufferService.save(
                    eventId,
                    environment,
                    topic,
                    key,
                    message,
                    e.getMessage()
            );
        }
    }

    private boolean isTopicConfigured(String environment, String topic) {
        return topicConfigRepository.findByEnvironmentAndTopic(environment, topic
                )
                .stream()
                .findFirst()
                .isPresent();
    }
}