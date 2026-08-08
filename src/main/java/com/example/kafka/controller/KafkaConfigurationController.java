
package com.example.kafka.controller;

import com.example.kafka.model.KafkaClusterProperty;
import com.example.kafka.model.KafkaTopicConfig;
import com.example.kafka.service.KafkaConfigurationService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/kafka/configuration")
@Validated
public class KafkaConfigurationController {

    private static final String ENVIRONMENT_REGEX =
            "^(DEV|HOM|PROD)$";

    private static final String UUID_REGEX =
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-" +
            "[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-" +
            "[0-9a-fA-F]{12}$";

    private final KafkaConfigurationService service;

    public KafkaConfigurationController(KafkaConfigurationService service) {
        this.service = service;
    }

    @PostMapping("/properties")
    public ResponseEntity<KafkaClusterProperty> saveProperties(@Valid @RequestBody KafkaClusterProperty request) {
        return ResponseEntity.ok(service.saveProperties(request)
        );
    }

    @GetMapping("/properties")
    public ResponseEntity<List<KafkaClusterProperty>> findAllProperties() {
        List<KafkaClusterProperty> properties = service.findAllProperties();
        if (properties.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(properties);
    }

    @GetMapping("/properties/{environment}")
    public ResponseEntity<KafkaClusterProperty> findProperties(
            @PathVariable
            @Pattern(regexp = ENVIRONMENT_REGEX, message = "{error.environment.invalid}"
            ) String environment) {

        return service.findProperties(environment)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build() );
    }

    @DeleteMapping("/properties/{uuid}")
    public ResponseEntity<Void> deleteProperties(
            @PathVariable
            @Pattern(regexp = UUID_REGEX, message = "{error.id.invalid_uuid}") String uuid) {

        return service.deleteProperties(uuid) ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }


    @PostMapping("/topics")
    public ResponseEntity<KafkaTopicConfig> saveTopic(@Valid @RequestBody KafkaTopicConfig request) {
        return ResponseEntity.ok(service.saveTopic(request));
    }

    @GetMapping("/topics")
    public ResponseEntity<List<KafkaTopicConfig>> findAllTopics() {
        List<KafkaTopicConfig> topics = service.findAllTopics();
        if (topics.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(topics);
    }

    @GetMapping("/topics/{environment}")
    public ResponseEntity<List<KafkaTopicConfig>> findTopics(
            @PathVariable
            @Pattern(regexp = ENVIRONMENT_REGEX, message = "{error.environment.invalid}")  String environment,
            @RequestParam(required = false) String name) {
        List<KafkaTopicConfig> topics = service.findTopics(environment, name);
        if (topics.isEmpty()) {
            return ResponseEntity.noContent().build();
        }
        return ResponseEntity.ok(topics);
    }

    @DeleteMapping("/topics/{uuid}")
    public ResponseEntity<Void> deleteTopic(
            @PathVariable
            @Pattern(regexp = UUID_REGEX, message = "{error.id.invalid_uuid}") String uuid) {
        return service.deleteTopic(uuid) ? ResponseEntity.noContent().build() : ResponseEntity.notFound().build();
    }
}
