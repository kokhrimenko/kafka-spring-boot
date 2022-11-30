package com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * Kafka message listener.
 *
 * @author kokhrime
 *
 */
@Slf4j
@Component
public class MessageListener {

	@KafkaListener(topics = "${message.topic.name}", groupId = "${message.listener.simple-group}")
	public void onMessage(ConsumerRecord<String, String> data) {
		log.info("Got new message with key: {}, payload: {}, partition: {}",
				data.key(), data.value(), data.partition());
	}
}
