package com.example.kafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record KafkaPublishDTO(
    @NotBlank(message = "{error.environment.required}")
    @Pattern(regexp = "^(DEV|HOM|PROD)$", message = "{error.environment.invalid}")
    String environment,

    @NotBlank(message = "{error.topic.required}")
    String topic,

    @NotBlank(message = "{error.key.required}")
    String key,

    @NotBlank(message = "{error.message.required}")
    String message
) {}
