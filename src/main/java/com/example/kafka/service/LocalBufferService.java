package com.example.kafka.service;

import com.example.kafka.model.LocalBuffer;
import com.example.kafka.repository.LocalBufferRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

@Service
public class LocalBufferService {

    private static final Logger log = LoggerFactory.getLogger(LocalBufferService.class);
    private static final long INITIAL_RETRY_DELAY_SECONDS = 30;
    private final LocalBufferRepository repository;
    public LocalBufferService(LocalBufferRepository repository) {
        this.repository = repository;
    }


    @Transactional
    public LocalBuffer save(
            String eventId,
            String environment,
            String topic,
            String key,
            String message,
            String error) {

        LocalBuffer buffer = new LocalBuffer();

        buffer.setEventId(eventId);
        buffer.setEnvironment(environment);
        buffer.setTopic(topic);
        buffer.setKey(key);
        buffer.setMessage(message);
        buffer.setLastError(error);

        buffer.setRetryCount(0);
        buffer.setStatus("PENDING");
        buffer.setNextRetry(LocalDateTime.now().plusSeconds(INITIAL_RETRY_DELAY_SECONDS));
        LocalBuffer saved = repository.save(buffer);
        log.info(
                "[LOCAL-BUFFER] Mensagem armazenada. id={} eventId={} environment={} topic={}",
                saved.getId(),
                saved.getEventId(),
                environment,
                topic
        );

        return saved;
    }

    @Transactional
    public void delete(Long id) {
        repository.deleteById(id);
        log.debug("[LOCAL-BUFFER] Mensagem {} removida.", id);
    }

    @Transactional
    public int resetErrors(String environment) {
        log.info("[LOCAL-BUFFER] Mensagens do ambiente {} resetadas para PENDING.", environment);
       return  repository.resetStatusByEnv(environment, "PENDING", 0, LocalDateTime.now());

    }
}