package com.example.kafka.controller;

import com.example.kafka.dto.KafkaPublishDTO;
import com.example.kafka.service.LocalBufferService;
import com.example.kafka.service.PublishService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka/publish")
public class PublishController {

    private final PublishService service;
    private final LocalBufferService localBufferService;

    public PublishController(PublishService service, LocalBufferService localBufferService) {
        this.service = service;
        this.localBufferService = localBufferService;
    }

    @PostMapping()
    public ResponseEntity<String> publish(@Valid @RequestBody KafkaPublishDTO dto) {
        service.publish(dto.environment(), dto.topic(),  dto.key(), dto.message());
        return ResponseEntity.ok("sent");
    }

    @PostMapping("reset/{env}")
    public ResponseEntity<String> resetBuffer(@PathVariable String env) {
        localBufferService.resetErrors(env);
        return ResponseEntity.ok("Mensagens de erro do ambiente " + env + " voltaram para a fila.");
    }
}