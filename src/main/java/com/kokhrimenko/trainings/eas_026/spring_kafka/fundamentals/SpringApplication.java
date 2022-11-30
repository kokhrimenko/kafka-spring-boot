package com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.annotation.EnableKafka;

import com.kokhrimenko.trainings.eas_026.spring_kafka.fundamentals.kafka.MessageProducer;

/**
 * Spring boot application class.
 *
 * @author kokhrime
 *
 */
@SpringBootApplication
@EnableKafka
public class SpringApplication {

	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext context = org.springframework.boot.SpringApplication.run(SpringApplication.class, args);

		MessageProducer producer = context.getBean(MessageProducer.class);
		producer.sendMessage("hello my new world!");
	}
}
