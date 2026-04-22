package com.example.kafka.config;

import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class ValidationConfig implements WebMvcConfigurer {

    // 1. O BEAN DO MESSAGE SOURCE (Define o local e o ENCODING dos arquivos)
    @Bean
    public MessageSource messageSource() {
        ResourceBundleMessageSource messageSource = new ResourceBundleMessageSource();
        messageSource.setBasename("messages"); // Procura arquivos iniciados com "messages"
        messageSource.setDefaultEncoding("UTF-8"); // Resolve o problema dos caracteres especiais ()
        messageSource.setUseCodeAsDefaultMessage(true);
        return messageSource;
    }

    // 2. O BEAN DO VALIDADOR (Conecta o Hibernate Validator ao MessageSource acima)
    @Bean
    @Override
    public LocalValidatorFactoryBean getValidator() {
        LocalValidatorFactoryBean bean = new LocalValidatorFactoryBean();
        bean.setValidationMessageSource(messageSource());
        return bean;
    }
}
