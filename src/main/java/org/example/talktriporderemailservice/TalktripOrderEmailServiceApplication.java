package org.example.talktriporderemailservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;

@SpringBootApplication
@EnableKafkaRetryTopic
public class TalktripOrderEmailServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TalktripOrderEmailServiceApplication.class, args);
    }

}
