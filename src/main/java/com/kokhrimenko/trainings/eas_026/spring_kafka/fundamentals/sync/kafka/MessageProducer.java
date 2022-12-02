package com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.sync.kafka;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

/**
 * Simple synchronous kafka message producer.
 *
 * @author kokhrime
 *
 */
@Component
public class MessageProducer {
	private static final Long WAITING_TIMEOUT = 20l;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Value(value = "${message.topic.name}")
	private String topicName;

	public SendResult<String, String> sendMessage(String message)
			throws InterruptedException, ExecutionException, TimeoutException {
		return kafkaTemplate.send(topicName, message).get(WAITING_TIMEOUT, TimeUnit.SECONDS);
	}

	public SendResult<String, String> sendMessage(String message, int partition)
			throws InterruptedException, ExecutionException, TimeoutException {
		return kafkaTemplate.send(topicName, partition, null, message).get(WAITING_TIMEOUT, TimeUnit.SECONDS);
	}

	public SendResult<String, String> sendMessage(String message, String key, int partition)
			throws InterruptedException, ExecutionException, TimeoutException {
		return kafkaTemplate.send(topicName, partition, key, message).get(WAITING_TIMEOUT, TimeUnit.SECONDS);
	}
}
