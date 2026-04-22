package com.example.kafka.model;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import jakarta.validation.constraints.Pattern;

import java.io.Serializable;

@Entity
@Table(name = "kafka_topic_config", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"env", "topic"})
})
public class KafkaTopicConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false, columnDefinition = "VARCHAR(36)")
    private String id;

    @Column
    @NotBlank(message = "{error.environment.required}")
    @Pattern(regexp = "^(DEV|HOM|PROD)$", message = "{error.environment.invalid}")
    private String environment;

    @Column
    @NotBlank(message = "{error.topic.required}")
    private String topic;

    @Convert(converter = JsonNodeConverter.class)
    @Column(columnDefinition = "TEXT")
    @NotNull(message = "{error.params.required}")
    private JsonNode parametros;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEnvironment() {
        return environment;
    }

    public void setEnvironment(String environment) {
        this.environment = environment;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public JsonNode getParametros() {
        return parametros;
    }

    public void setParametros(JsonNode parametros) {
        this.parametros = parametros;
    }
}
