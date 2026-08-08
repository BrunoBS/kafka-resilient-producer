package com.example.kafka.service;

import com.example.kafka.repository.KafkaClusterPropertyRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class KafkaAdminService {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaAdminService.class);

    private final KafkaClusterPropertyRepository clusterRepository;
    private final KafkaProducerRegistry registry;

    public KafkaAdminService(
            KafkaClusterPropertyRepository clusterRepository,
            KafkaProducerRegistry registry) {

        this.clusterRepository = clusterRepository;
        this.registry = registry;
    }

    /**
     * Recarrega os recursos Kafka de um ambiente.
     * <p>
     * O reload remove o Producer e o AdminClient atualmente
     * mantidos pelo Registry.
     * <p>
     * Os novos recursos serão criados automaticamente,
     * utilizando as configurações atualmente existentes no banco,
     * quando forem solicitados novamente.
     *
     * @param environment ambiente Kafka (DEV, HOM ou PROD)
     * @return true se o ambiente existir; false caso contrário
     */
    @Transactional(readOnly = true)
    public boolean reload(String environment) {
        log.info("[KAFKA-ADMIN] Solicitação de reload para o ambiente {}", environment
        );
        boolean exists = clusterRepository.findByEnvironment(environment).isPresent();
        if (!exists) {
            log.warn("[KAFKA-ADMIN] Ambiente {} não encontrado.", environment);
            return false;
        }
        registry.reload(environment);
        log.info("[KAFKA-ADMIN] Reload concluído para o ambiente {}", environment);

        return true;
    }
}

