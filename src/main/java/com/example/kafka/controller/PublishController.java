package com.example.kafka.controller;

import com.example.kafka.dto.KafkaPublishDTO;
import com.example.kafka.service.PublishService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/kafka/publish")
public class PublishController {

    private final PublishService publishService;

    public PublishController(PublishService publishService) {
        this.publishService = publishService;
    }

    @PostMapping
    public ResponseEntity<String> publish(
            @Valid @RequestBody List<KafkaPublishDTO> dtos) {
/*
                    Kafka-resilient-producer
                         │
                         │ recebe lista
                         ▼
     Para cada dto deverá chamar a api de eventos para publicar  o status da chave
                  ┌─────────────┐
                  │ PUBLISHING  │
                  └──────┬──────┘
                         │
                     tenta Kafka
                    /          \
                   /            \
        sucesso   /        erro  \
                 │                │
                 ▼                ▼
             PUBLISHED           ERROR

             P
        */


        dtos.forEach(dto -> {
            publishService.publish(
                    dto.environment(),
                    dto.topic(),
                    dto.key(),
                    dto.message()
            );
        });
        return ResponseEntity
                .accepted()
                .body("Message accepted for processing");
    }
}
