package org.example.talktriporderemailservice.messaging.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 결제 성공 메일 처리 최종 실패 시 RetryableTopic 이 라우팅한 DLT 레코드를 수신합니다.
 */
@Component
public class PaymentSuccessEmailDltConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PaymentSuccessEmailDltConsumer.class);

    @KafkaListener(
            topics = "${kafka.topics.payment-success:payment-success}.dlt",
            groupId = "talktrip-order-email-service"
    )
    public void onPaymentSuccessDlt(
            @Payload Map<String, Object> payload,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        Object orderId = payload != null ? payload.get("orderId") : null;
        Object orderCode = payload != null ? payload.get("orderCode") : null;
        Object memberEmail = payload != null ? payload.get("memberEmail") : null;

        logger.error(
                "[order-email][DLT] payment-success 처리 실패(최종). orderId={}, orderCode={}, memberEmail={}, topic={}, partition={}, offset={}, key={}, payload={}",
                orderId, orderCode, memberEmail, topic, partition, offset, key, payload);
    }
}
