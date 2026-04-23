package org.example.talktriporderemailservice.email.domain;

import jakarta.persistence.Column;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import jakarta.persistence.Entity;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(
        name = "email_recipient_log",
        indexes = {
                @Index(name = "idx_email_recipient_log_recipient_created", columnList = "recipient_email,created_at"),
                @Index(name = "idx_email_recipient_log_status_created", columnList = "status,created_at"),
                @Index(name = "idx_email_recipient_log_order_code", columnList = "order_code")
        }
)
@Entity
public class EmailRecipientLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "recipient_email", nullable = false, length = 320)
    private String recipientEmail;

    @Column(name = "subject", nullable = false, length = 255)
    private String subject;

    @Lob
    @Column(name = "body", nullable = false)
    private String body;

    @Column(name = "order_id")
    private Long orderId;

    @Column(name = "order_code", length = 100)
    private String orderCode;

    @Column(name = "kafka_key", length = 255)
    private String kafkaKey;

    @Column(name = "kafka_topic", length = 255)
    private String kafkaTopic;

    @Column(name = "kafka_partition")
    private Integer kafkaPartition;

    @Column(name = "kafka_offset")
    private Long kafkaOffset;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private EmailRecipientLogStatus status;

    @Lob
    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Builder
    private EmailRecipientLog(
            String recipientEmail,
            String subject,
            String body,
            Long orderId,
            String orderCode,
            String kafkaKey,
            String kafkaTopic,
            Integer kafkaPartition,
            Long kafkaOffset,
            EmailRecipientLogStatus status,
            String errorMessage
    ) {
        this.recipientEmail = recipientEmail;
        this.subject = subject;
        this.body = body;
        this.orderId = orderId;
        this.orderCode = orderCode;
        this.kafkaKey = kafkaKey;
        this.kafkaTopic = kafkaTopic;
        this.kafkaPartition = kafkaPartition;
        this.kafkaOffset = kafkaOffset;
        this.status = status;
        this.errorMessage = errorMessage;
    }

    @PrePersist
    void prePersist() {
        if (createdAt == null) createdAt = Instant.now();
    }

    public void updateMessage(String recipientEmail, String subject, String body) {
        this.recipientEmail = recipientEmail;
        this.subject = subject;
        this.body = body;
    }

    public void markReceived() {
        this.status = EmailRecipientLogStatus.RECEIVED;
        this.errorMessage = null;
    }

    public void markSent() {
        this.status = EmailRecipientLogStatus.SENT;
        this.errorMessage = null;
    }

    public void markSkipped(String errorMessage) {
        this.status = EmailRecipientLogStatus.SKIPPED;
        this.errorMessage = errorMessage;
    }

    public void markFailed(String errorMessage) {
        this.status = EmailRecipientLogStatus.FAILED;
        this.errorMessage = errorMessage;
    }
}

