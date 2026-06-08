package org.example.talktriporderemailservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * {@link org.springframework.kafka.annotation.EnableKafkaRetryTopic} +
 * {@link org.springframework.kafka.annotation.RetryableTopic} 조합은
 * 파티션 일시 정지(retry backoff)용 스케줄러가 필요합니다.
 * 없으면: {@code Either a RetryTopicSchedulerWrapper or TaskScheduler bean is required}
 */
@Configuration
public class KafkaRetryTopicSchedulingConfig {

    @Bean
    public TaskScheduler kafkaRetryTopicTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("kafka-retry-topic-");
        scheduler.setRemoveOnCancelPolicy(true);
        scheduler.initialize();
        return scheduler;
    }
}
