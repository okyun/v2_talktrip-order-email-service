package org.example.talktriporderemailservice.messaging.consumer;

import org.example.talktriporderemailservice.email.domain.EmailRecipientLog;
import org.example.talktriporderemailservice.email.domain.EmailRecipientLogStatus;
import org.example.talktriporderemailservice.email.repository.EmailRecipientLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

/**
 * 결제 성공 이벤트를 수신해서 이메일 발송 트리거로 사용하는 Consumer.
 *
 * 현재는 이메일 발송 구현 대신 수신 로그만 남깁니다.
 * (SMTP_USERNAME/SMTP_PASSWORD 등 만 넣으면 실제로 메일이 나갑니다.)
 */
@Component
public class PaymentSuccessEmailConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PaymentSuccessEmailConsumer.class);

    private final JavaMailSender mailSender;
    private final EmailRecipientLogRepository emailRecipientLogRepository;

    public PaymentSuccessEmailConsumer(JavaMailSender mailSender, EmailRecipientLogRepository emailRecipientLogRepository) {
        this.mailSender = mailSender;
        this.emailRecipientLogRepository = emailRecipientLogRepository;
    }

    @KafkaListener(
            topics = "${kafka.topics.payment-success:payment-success}",
            groupId = "talktrip-order-email-service",
            concurrency = "1"
    )
    public void onPaymentSuccess(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        Object orderId = payload != null ? payload.get("orderId") : null;
        Object orderCode = payload != null ? payload.get("orderCode") : null;
        Object memberEmail = payload != null ? payload.get("memberEmail") : null;

        logger.info("payment-success 수신(email): orderId={}, orderCode={}, memberEmail={}, topic={}, partition={}, offset={}, key={}",
                orderId, orderCode, memberEmail, topic, partition, offset, key);

        Long parsedOrderId = parseLongOrNull(orderId);
        String parsedOrderCode = orderCode != null ? orderCode.toString() : null;
        String to = memberEmail != null ? memberEmail.toString() : null;

        // 이메일 주소가 있어야 실제 발송 가능
        if (to == null || to.isBlank()) {
            logger.warn("이메일 발송 스킵: memberEmail 없음. orderId={}, orderCode={}", orderId, orderCode);
            emailRecipientLogRepository.save(Objects.requireNonNull(EmailRecipientLog.builder()
                    .recipientEmail("(blank)")
                    .subject("[TalkTrip] 주문이 완료되었습니다")
                    .body("SKIPPED: memberEmail 없음")
                    .orderId(parsedOrderId)
                    .orderCode(parsedOrderCode)
                    .kafkaKey(key)
                    .kafkaTopic(topic)
                    .kafkaPartition(partition)
                    .kafkaOffset(offset)
                    .status(EmailRecipientLogStatus.SKIPPED)
                    .errorMessage("memberEmail is blank")
                    .build()));
            return;
        }

        SimpleMailMessage message = new SimpleMailMessage();
        message.setTo(to);
        message.setSubject("[TalkTrip] 주문이 완료되었습니다");
        message.setText("주문이 완료되었습니다.\n\n- 주문번호: " + orderCode + "\n- 주문 ID: " + orderId + "\n");

        try {
            mailSender.send(message);
            emailRecipientLogRepository.save(Objects.requireNonNull(EmailRecipientLog.builder()
                    .recipientEmail(to)
                    .subject(message.getSubject())
                    .body(message.getText())
                    .orderId(parsedOrderId)
                    .orderCode(parsedOrderCode)
                    .kafkaKey(key)
                    .kafkaTopic(topic)
                    .kafkaPartition(partition)
                    .kafkaOffset(offset)
                    .status(EmailRecipientLogStatus.SENT)
                    .build()));
            logger.info("이메일 발송 완료: to={}, orderCode={}", to, orderCode);
        } catch (Exception e) {
            emailRecipientLogRepository.save(Objects.requireNonNull(EmailRecipientLog.builder()
                    .recipientEmail(to)
                    .subject(message.getSubject())
                    .body(message.getText())
                    .orderId(parsedOrderId)
                    .orderCode(parsedOrderCode)
                    .kafkaKey(key)
                    .kafkaTopic(topic)
                    .kafkaPartition(partition)
                    .kafkaOffset(offset)
                    .status(EmailRecipientLogStatus.FAILED)
                    .errorMessage(e.getMessage())
                    .build()));
            logger.error("이메일 발송 실패: to={}, orderCode={}", to, orderCode, e);
        }
    }

    private static Long parseLongOrNull(Object value) {
        if (value == null) return null;
        if (value instanceof Number n) return n.longValue();
        try {
            String s = value.toString();
            if (s.isBlank()) return null;
            return Long.parseLong(s);
        } catch (Exception ignored) {
            return null;
        }
    }
}

