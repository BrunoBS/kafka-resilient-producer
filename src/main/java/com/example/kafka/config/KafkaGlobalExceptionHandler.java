
package com.example.kafka.config;

import jakarta.validation.ConstraintViolationException;
import org.springframework.context.MessageSource;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@RestControllerAdvice
public class KafkaGlobalExceptionHandler {

    private final MessageSource messageSource;

    // Injeção do MessageSource para buscar nos arquivos .properties
    public KafkaGlobalExceptionHandler(MessageSource messageSource) {
        this.messageSource = messageSource;
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<Map<String, Object>> handleConstraintViolation(ConstraintViolationException ex) {
        // Pega a mensagem da primeira violação (já traduzida pelo Bean Validation se o config estiver OK)
        String message = ex.getConstraintViolations().iterator().next().getMessage();
        return createResponse(HttpStatus.BAD_REQUEST, message);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        // Pega a mensagem do primeiro erro do DTO
        String errorMessage = ex.getBindingResult().getAllErrors().get(0).getDefaultMessage();
        return createResponse(HttpStatus.BAD_REQUEST, errorMessage);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        return createResponse(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> handleJsonError(HttpMessageNotReadableException ex) {
        // Busca a tradução da chave 'error.json.malformed' dinamicamente
        String message = getMessage("error.json.malformed", "JSON malformado ou inválido.");
        return createResponse(HttpStatus.BAD_REQUEST, message);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneralError(Exception ex) {
        // Logue o erro real no console/Datadog para você debugar, mas oculte do usuário final
        System.err.println("[CRITICAL-ERROR] " + ex.getMessage());

        String message = getMessage("error.generic.unexpected", "Erro inesperado no servidor.");
        return createResponse(HttpStatus.INTERNAL_SERVER_ERROR, message);
    }

    // Método auxiliar para buscar no messageSource com o Locale atual
    private String getMessage(String code, String defaultMessage) {
        return messageSource.getMessage(code, null, defaultMessage, LocaleContextHolder.getLocale());
    }

    private ResponseEntity<Map<String, Object>> createResponse(HttpStatus status, String message) {
        Map<String, Object> body = new HashMap<>();
        body.put("timestamp", LocalDateTime.now());
        body.put("status", status.value());
        body.put("error", status.getReasonPhrase());
        body.put("message", message);
        return new ResponseEntity<>(body, status);
    }
}
