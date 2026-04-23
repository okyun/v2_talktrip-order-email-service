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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;

/**
 * 결제 성공(payment-success) 이벤트를 수신해 주문 완료 메일을 보내는 Consumer.
 */
@Component
public class PaymentSuccessEmailConsumer {

    private static final String FROM_EMAIL = "dhrdbs8066@naver.com";

    private static final Logger logger = LoggerFactory.getLogger(PaymentSuccessEmailConsumer.class);

    private final JavaMailSender mailSender;
    private final EmailRecipientLogRepository emailRecipientLogRepository;
    private final boolean emailSendEnabled;

    public PaymentSuccessEmailConsumer(
            JavaMailSender mailSender,
            EmailRecipientLogRepository emailRecipientLogRepository,
            @Value("${talktrip.email.send-enabled:false}") boolean emailSendEnabled
    ) {
        this.mailSender = mailSender;
        this.emailRecipientLogRepository = emailRecipientLogRepository;
        this.emailSendEnabled = emailSendEnabled;
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

        logger.info(
                "[order-email] RECEIVED payment-success. orderId={}, orderCode={}, memberEmail={}, topic={}, partition={}, offset={}, key={}",
                orderId, orderCode, memberEmail, topic, partition, offset, key);

        Long parsedOrderId = parseLongOrNull(orderId);
        String parsedOrderCode = orderCode != null ? orderCode.toString() : null;
        String to = memberEmail != null ? memberEmail.toString() : null;

        String subject = "[TalkTrip] 주문이 완료되었습니다";
        String body = "주문이 완료되었습니다.\n\n- 주문번호: " + orderCode + "\n- 주문 ID: " + orderId + "\n";

        // 결제 성공 이벤트를 받자마자 DB에 흔적 남김 (발송 성공/실패/스킵 여부는 이후 업데이트)
        EmailRecipientLog log = emailRecipientLogRepository.save(Objects.requireNonNull(EmailRecipientLog.builder()
                .recipientEmail((to == null || to.isBlank()) ? "(blank)" : to)
                .subject(subject)
                .body(body)
                .orderId(parsedOrderId)
                .orderCode(parsedOrderCode)
                .kafkaKey(key)
                .kafkaTopic(topic)
                .kafkaPartition(partition)
                .kafkaOffset(offset)
                .status(EmailRecipientLogStatus.RECEIVED)
                .build()));

        logger.info("[order-email] RECEIVED DB저장됨. logId={}, orderId={}, orderCode={}", log.getId(), orderId, orderCode);

        // 개발/로컬 테스트용: SMTP 발송을 끄고 DB 로그만 남김
        if (!emailSendEnabled) {
            log.markSkipped("email sending disabled (talktrip.email.send-enabled=false)");
            emailRecipientLogRepository.save(log);
            logger.info(
                    "[order-email] SKIPPED(발송비활성) DB업데이트됨. logId={}, orderId={}, orderCode={}",
                    log.getId(), orderId, orderCode);
            return;
        }

        // 이메일 주소가 있어야 실제 발송 가능
        if (to == null || to.isBlank()) {
            logger.warn(
                    "[order-email] SKIPPED: memberEmail 비어 있음. orderId={}, orderCode={}, topic={}, partition={}, offset={}",
                    orderId, orderCode, topic, partition, offset);
            log.updateMessage("(blank)", subject, "SKIPPED: memberEmail 없음");
            log.markSkipped("memberEmail is blank");
            emailRecipientLogRepository.save(log);
            logger.info("[order-email] SKIPPED DB업데이트됨. logId={}, orderId={}, orderCode={}", log.getId(), orderId, orderCode);
            return;
        }

        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(FROM_EMAIL);
        message.setTo(to);
        message.setSubject(subject);
        message.setText(body);

        logger.info(
                "[order-email] SEND 시도. from={}, to={}, subject={}, bodyChars={}, orderId={}, orderCode={}, kafka=topic={} part={} offset={} key={}",
                FROM_EMAIL, to, subject, body.length(), orderId, orderCode, topic, partition, offset, key);

        try {
            long startedNs = System.nanoTime();
            mailSender.send(message);
            long elapsedMs = (System.nanoTime() - startedNs) / 1_000_000L;
            log.updateMessage(to, subject, body);
            log.markSent();
            emailRecipientLogRepository.save(log);
            logger.info(
                    "[order-email] SENT smtpOK=true, dbLog=UPDATED, elapsedMs={}, logId={}, from={}, to={}, orderId={}, orderCode={}",
                    elapsedMs, log.getId(), FROM_EMAIL, to, orderId, orderCode);
        } catch (Exception e) {
            log.updateMessage(to, subject, body);
            log.markFailed(e.getMessage());
            emailRecipientLogRepository.save(log);
            logger.error(
                    "[order-email] FAILED logId={}, from={}, to={}, orderId={}, orderCode={}, exClass={}, exMsg={}",
                    log.getId(), FROM_EMAIL, to, orderId, orderCode, e.getClass().getSimpleName(), e.getMessage(), e);
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

