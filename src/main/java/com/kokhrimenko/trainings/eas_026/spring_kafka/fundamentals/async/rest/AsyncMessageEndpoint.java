package com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.async.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.async.kafka.AsyncMessageProducer;

/**
 * Asynchronous rest controller to send some user messages to the Kafka topic.
 *
 * @author kokhrime
 *
 */
@RestController
@RequestMapping("/async/messages")
public class AsyncMessageEndpoint {
	@Autowired
	private AsyncMessageProducer messageProducer;

	@PostMapping(value = "/send")
	@ResponseStatus(code = HttpStatus.OK, reason = "successfully sent")
	public void send(@RequestBody String message) {
		messageProducer.sendMessage(message);
	}

	@PostMapping(value = "/send/{partition}")
	@ResponseStatus(code = HttpStatus.OK, reason = "successfully sent")
	public void send(@PathVariable int partition, @RequestBody String message) {
		messageProducer.sendMessage(message, partition);
	}

	@PostMapping(value = "/send/{partition}/{key}")
	@ResponseStatus(code = HttpStatus.OK, reason = "successfully sent")
	public void send(@PathVariable int partition, @PathVariable String key, @RequestBody String message) {
		messageProducer.sendMessage(message, key, partition);
	}
}
