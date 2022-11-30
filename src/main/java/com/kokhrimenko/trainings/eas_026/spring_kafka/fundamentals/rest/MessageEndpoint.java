package com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.kafka.MessageProducer;

/**
 * Rest controller to send some user messages to the Kafka topic.
 *
 * @author kokhrime
 *
 */
@RestController
@RequestMapping("/messages")
public class MessageEndpoint {
	@Autowired
	private MessageProducer messageProducer;

	@PostMapping(value = "/send")
	public void send(@RequestBody String message) {
		messageProducer.sendMessage(message);
	}

	@PostMapping(value = "/send/{partition}")
	public void send(@PathVariable int partition, @RequestBody String message) {
		messageProducer.sendMessage(message, partition);
	}

	@PostMapping(value = "/send/{partition}/{key}")
	public void send(@PathVariable int partition, @PathVariable String key, @RequestBody String message) {
		messageProducer.sendMessage(message, key, partition);
	}
}
