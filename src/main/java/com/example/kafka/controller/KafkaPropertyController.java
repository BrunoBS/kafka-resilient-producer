package com.example.kafka.controller;

import com.example.kafka.model.KafkaClusterProperty;
import com.example.kafka.model.KafkaTopicConfig;
import com.example.kafka.repository.KafkaClusterPropertyRepository;
import com.example.kafka.repository.KafkaTopicConfigRepository;
import com.example.kafka.service.KafkaProducerRegistry;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/kafka")
@Validated
public class KafkaPropertyController {
    private final KafkaTopicConfigRepository kafkaTopicConfigRepository;
    private final KafkaClusterPropertyRepository kafkaClusterPropertyRepository;
    private final KafkaProducerRegistry registry;

    public KafkaPropertyController(KafkaTopicConfigRepository kafkaTopicConfigRepository,
                                   KafkaClusterPropertyRepository kafkaClusterPropertyRepository,
                                   KafkaProducerRegistry registry) {
        this.kafkaTopicConfigRepository = kafkaTopicConfigRepository;
        this.kafkaClusterPropertyRepository = kafkaClusterPropertyRepository;

        this.registry = registry;

    }

    @PostMapping("/properties")
    public ResponseEntity<KafkaClusterProperty> save(
            @Valid @RequestBody KafkaClusterProperty dto) {
        KafkaClusterProperty entity = kafkaClusterPropertyRepository.findByEnvironment(dto.getEnvironment()).orElse(new KafkaClusterProperty());
        entity.setEnvironment(dto.getEnvironment());
        entity.setProperties(dto.getProperties());
        KafkaClusterProperty save = kafkaClusterPropertyRepository.save(entity);
        return ResponseEntity.ok(save);
    }

    @GetMapping("/properties")
    public ResponseEntity<List<KafkaClusterProperty>> findAllProperties() {
        List<KafkaClusterProperty> list = kafkaClusterPropertyRepository.findAll();
        return list.isEmpty() ? ResponseEntity.noContent().build() : ResponseEntity.ok(list);
    }


    @GetMapping("/properties/{environment}")
    public ResponseEntity<KafkaClusterProperty> findByPropertiesEnvironment(
            @PathVariable @Pattern(regexp = "^(DEV|HOM|PROD)$", message = "{error.environment.invalid}") String environment
    ) {
        return kafkaClusterPropertyRepository.findByEnvironment(environment)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @DeleteMapping("/properties/{uuid}")
    public ResponseEntity<Void> deleteProperties(
            @Pattern(regexp = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
                    message = "{error.id.invalid_uuid}")
            @PathVariable String uuid) {
        return kafkaClusterPropertyRepository.findById(uuid)
                .map(topicConfig -> {
                    kafkaClusterPropertyRepository.delete(topicConfig);
                    return ResponseEntity.noContent().<Void>build();
                })
                .orElse(ResponseEntity.notFound().build());
    }


    @PostMapping("/topics")
    public ResponseEntity<KafkaTopicConfig> save(@Valid @RequestBody KafkaTopicConfig dto) {
        dto.setId(null);
        KafkaTopicConfig entity = kafkaTopicConfigRepository.findByEnvironmentAndTopic(dto.getEnvironment(), dto.getTopic()).stream().findFirst()
                .orElse(new KafkaTopicConfig());

        entity.setEnvironment(dto.getEnvironment());
        entity.setTopic(dto.getTopic());
        entity.setParametros(dto.getParametros());
        KafkaTopicConfig save = kafkaTopicConfigRepository.save(entity);
        return ResponseEntity.ok(save);
    }

    @GetMapping("/topics")
    public ResponseEntity<List<KafkaTopicConfig>> findAllTopics() {
        List<KafkaTopicConfig> list = kafkaTopicConfigRepository.findAll();
        return list.isEmpty() ? ResponseEntity.noContent().build() : ResponseEntity.ok(list);
    }


    @GetMapping("/topics/{environment}")
    public ResponseEntity<List<KafkaTopicConfig>> findAllTopicsEnvironment(
            @PathVariable @Pattern(regexp = "^(DEV|HOM|PROD)$", message = "{error.environment.invalid}") String environment,
            @RequestParam(required = false) String name
    ) {
        List<KafkaTopicConfig> topics = (name == null ?
                kafkaTopicConfigRepository.findByEnvironment(environment) :
                kafkaTopicConfigRepository.findByEnvironmentAndTopic(environment, name));
        return topics.isEmpty()
                ? ResponseEntity.noContent().build()
                : ResponseEntity.ok(topics);
    }

    @DeleteMapping("/topics/{uuid}")
    public ResponseEntity<Void> deleteTopics(
            @Pattern(regexp = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
                    message = "{error.id.invalid_uuid}")
            @PathVariable String uuid) {
        return kafkaTopicConfigRepository.findById(uuid)
                .map(topicConfig -> {
                    kafkaTopicConfigRepository.delete(topicConfig);
                    return ResponseEntity.noContent().<Void>build();
                })
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/reload/{environment}")
    public ResponseEntity<Void> reload(
            @PathVariable @Pattern(regexp = "^(DEV|HOM|PROD)$", message = "{error.environment.invalid}") String environment
    ) {
        return kafkaClusterPropertyRepository.findByEnvironment(environment)
                .map(entity -> {
                    if (entity.getProperties() != null && entity.getProperties().has("bootstrap.servers")) {
                        registry.reloadProducer(environment);
                        return ResponseEntity.noContent().<Void>build();
                    }
                    return ResponseEntity.badRequest().<Void>build();
                })
                .orElse(ResponseEntity.notFound().build());
    }
}