package com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import lombok.extern.slf4j.Slf4j;

/**
 * Simple Kafka message producer.
 *
 * @author kokhrime
 *
 */
@Component
@Slf4j
public class MessageProducer {
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Value(value = "${message.topic.name}")
	private String topicName;

	public void sendMessage(String message) {
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
			@Override
			public void onSuccess(SendResult<String, String> result) {
				log.info("Message=[{}] with offset=[{}] was successfully sent", message, result.getRecordMetadata().offset());
			}
			@Override
			public void onFailure(Throwable ex) {
				log.error("Unable to send message=[{}] due to: ", message, ex);
			}
		});
	}

	public void sendMessage(String message, int partition) {
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, partition, null, message);
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
			@Override
			public void onSuccess(SendResult<String, String> result) {
				log.info("Message=[{}] with offset=[{}] was successfully sent to partition=[{}]",
						message, result.getRecordMetadata().offset(), partition);
			}
			@Override
			public void onFailure(Throwable ex) {
				log.error("Unable to send message=[{}] to partition=[{}] due to: ", message, partition, ex);
			}
		});
	}

	public void sendMessage(String message, String key, int partition) {
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, partition, key, message);
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
			@Override
			public void onSuccess(SendResult<String, String> result) {
				log.info("Message=[{}]; key=[{}] with offset=[{}] was successfully sent to partition=[{}]",
						message, key, result.getRecordMetadata().offset(), partition);
			}
			@Override
			public void onFailure(Throwable ex) {
				log.error("Unable to send message=[{}] to partition=[{}] due to: ", message, partition, ex);
			}
		});
	}
}
