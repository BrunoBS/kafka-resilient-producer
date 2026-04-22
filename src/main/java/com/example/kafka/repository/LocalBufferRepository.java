package com.example.kafka.repository;

import com.example.kafka.model.LocalBuffer;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface LocalBufferRepository extends JpaRepository<LocalBuffer, Long> {

    @Query("SELECT b FROM LocalBuffer b WHERE b.environment = :environment " +
            "AND b.status IN ('PENDING', 'FAILED_RETRY') " +
            "AND b.nextRetry <= :now " +
            "ORDER BY b.id ASC")
    List<LocalBuffer> findOldestPending(@Param("environment") String environment,
                                        @Param("now") LocalDateTime now,
                                        Pageable pageable);

    @Retryable(
            retryFor = {org.springframework.dao.RecoverableDataAccessException.class,
                    org.hibernate.exception.LockAcquisitionException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 500)
    )
    @Modifying
    @Transactional
    @Query("UPDATE LocalBuffer b SET b.nextRetry = :now " +
            "WHERE b.environment = :environment " +
            "AND b.status IN ('PENDING', 'FAILED_RETRY') " +
            "AND b.nextRetry > :now")
    void fastTrackByEnv(@Param("environment") String environment, @Param("now") LocalDateTime now);

    @Retryable(retryFor = {org.hibernate.exception.LockAcquisitionException.class}, maxAttempts = 3, backoff = @Backoff(delay = 500))
    @Modifying
    @Transactional
    @Query("UPDATE LocalBuffer b SET b.status = :newStatus, b.retryCount = :retryCount, b.nextRetry = :nextRetry " +
            "WHERE b.environment = :environment AND b.status = 'ERROR'")
    void resetStatusByEnv(@Param("environment") String environment,
                          @Param("newStatus") String newStatus,
                          @Param("retryCount") int retryCount,
                          @Param("nextRetry") LocalDateTime nextRetry);

    @Retryable(retryFor = {org.hibernate.exception.LockAcquisitionException.class}, maxAttempts = 2, backoff = @Backoff(delay = 1000))
    @Modifying
    @Transactional
    @Query("DELETE FROM LocalBuffer b WHERE b.status = :status AND b.updatedAt < :date")
    int deleteExpiredErrors(@Param("status") String status, @Param("date") LocalDateTime date);

    @Retryable(retryFor = {org.hibernate.exception.LockAcquisitionException.class}, maxAttempts = 2, backoff = @Backoff(delay = 1000))
    @Modifying
    @Transactional
    @Query("DELETE FROM LocalBuffer b WHERE b.updatedAt < :date")
    int deleteVeryOld(@Param("date") LocalDateTime date);

    @Retryable(
            retryFor = {
                    org.springframework.dao.TransientDataAccessException.class,
                    org.hibernate.exception.LockAcquisitionException.class
            },
            maxAttempts = 3,
            backoff = @Backoff(delay = 500)
    )
    <S extends LocalBuffer> S save(S entity);
}
