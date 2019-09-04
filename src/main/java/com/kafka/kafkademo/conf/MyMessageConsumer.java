package com.kafka.kafkademo.conf;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static org.springframework.kafka.support.KafkaHeaders.TOPIC;

/**
 * Created by Felix Porres on 30/08/2019.
 */
@Component
public class MyMessageConsumer {

    @KafkaListener(topics = TOPIC, groupId = "KAFKA_GROUP_ID")
    public void receive(String message){
        System.out.println("Received Messasge in group KAFKA_GROUP_ID: " + message);
    }
}
