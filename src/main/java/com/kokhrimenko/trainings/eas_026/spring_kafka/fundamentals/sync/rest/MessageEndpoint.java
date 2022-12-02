package com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.sync.rest;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.sync.kafka.MessageProducer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Rest controller to send some user messages to the Kafka topic.
 *
 * @author kokhrime
 *
 */
@RestController
@RequestMapping("/messages")
@Slf4j
public class MessageEndpoint {

	@Getter
	private static class MessageResponse {
		private final String topic;
		private final Integer partition;
		private final Long timestamp;
		private final long offset;

		public MessageResponse(final SendResult<String, String> result) {
			this.topic = result.getProducerRecord().topic();
			this.partition = result.getRecordMetadata().partition();
			this.timestamp = result.getProducerRecord().timestamp();
			this.offset = result.getRecordMetadata().offset();
		}
	}

	@Autowired
	private MessageProducer messageProducer;

	@PostMapping(value = "/send", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseStatus(code = HttpStatus.OK)
	public MessageResponse send(@RequestBody String message, HttpServletResponse response) throws IOException {
		try {
			return new MessageResponse(messageProducer.sendMessage(message));
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			log.error("Cannot send message. Original exception: {}", e);
			response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
		}
		return null;
	}

	@PostMapping(value = "/send/{partition}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseStatus(code = HttpStatus.OK)
	public MessageResponse send(@PathVariable int partition, @RequestBody String message, HttpServletResponse response) throws IOException {
		try {
			return new MessageResponse(messageProducer.sendMessage(message, partition));
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			log.error("Cannot send message. Original exception: {}", e);
			response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
		}
		return null;
	}

	@PostMapping(value = "/send/{partition}/{key}", produces = MediaType.APPLICATION_JSON_VALUE)
	@ResponseStatus(code = HttpStatus.OK)
	public MessageResponse send(@PathVariable int partition, @PathVariable String key, @RequestBody String message, HttpServletResponse response) throws IOException {
		try {
			return new MessageResponse(messageProducer.sendMessage(message, key, partition));
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			log.error("Cannot send message. Original exception: {}", e);
			response.sendError(HttpStatus.INTERNAL_SERVER_ERROR.value());
		}
		return null;
	}
}
