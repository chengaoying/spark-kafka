package com.macro.test.kafka;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RDDKafkaWriter implements Serializable {

	private static final long serialVersionUID = 7374381310562055607L;
	private final KafkaProducerPool pool;
	private final Boolean async;

	public RDDKafkaWriter(final KafkaProducerPool pool, Boolean async) {
		this.pool = pool;
		this.async = async;
	}

	public void writeToKafka(String topic,String message) {
		KafkaProducer<String, String> producer = pool.borrowProducer();
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(
				topic, message);
		if (async) {
			producer.send(record);
		} else {
			try {
				producer.send(record).get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
		pool.returnProducer(producer);
	}
}
