package com.example.kafka.service;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaMessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessagePublisher.class);
    private static final String EVENT_ID_HEADER = "eventId";
    private static final int SEND_TIMEOUT_SECONDS = 3;
    private final KafkaProducerRegistry registry;

    public KafkaMessagePublisher(KafkaProducerRegistry registry) {
        this.registry = registry;
    }

    public RecordMetadata publish(String eventId, String environment, String topic, String key, String message) throws Exception {
        var producer = registry.get(environment);
        if (producer == null) {
            throw new IllegalStateException("Producer não disponível para o ambiente: " + environment);
        }
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        record.headers().add(EVENT_ID_HEADER, eventId.getBytes(StandardCharsets.UTF_8));
        RecordMetadata metadata = producer.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        log.debug(
                "[KAFKA-PUBLISH] eventId={} environment={} topic={} partition={} offset={}",
                eventId,
                environment,
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
        );
        return metadata;
    }

    public boolean isHealthy(String environment) {
        return registry.isHealthy(environment);
    }
}