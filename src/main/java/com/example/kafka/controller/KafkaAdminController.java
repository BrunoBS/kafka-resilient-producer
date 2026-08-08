
package com.example.kafka.controller;

import com.example.kafka.service.KafkaAdminService;
import com.example.kafka.service.LocalBufferService;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kafka/admin")
@Validated
public class KafkaAdminController {

    private static final String ENVIRONMENT_REGEX = "^(DEV|HOM|PROD)$";

    private final KafkaAdminService service;
    private final LocalBufferService localBufferService;

    public KafkaAdminController(KafkaAdminService service, LocalBufferService localBufferService) {
        this.service = service;
        this.localBufferService = localBufferService;
    }

    @PostMapping("/reload/{environment}")
    public ResponseEntity<Void> reload(
            @PathVariable
            @Pattern(regexp = ENVIRONMENT_REGEX, message = "{error.environment.invalid}") String environment) {

        boolean exists = service.reload(environment);
        return (!exists) ? ResponseEntity.notFound().build() : ResponseEntity.noContent().build();
    }

    @PostMapping("/reset/{environment}")
    public ResponseEntity<String> reset(@PathVariable
                                        @Pattern(regexp = ENVIRONMENT_REGEX, message = "{error.environment.invalid}") String environment) {
        int updated = localBufferService.resetErrors(environment);
        return ResponseEntity.ok("Mensagens resetadas: " + updated
        );
    }
}