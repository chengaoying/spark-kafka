package com.macro.test.kafka;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.macro.test.util.ConfigurationManager;

public class RDDKafkaWriter implements Serializable {

	private static final long serialVersionUID = 7374381310562055607L;
	private final KafkaProducerPool pool;
	private final Boolean async;
	protected static String kafka_topic = ConfigurationManager.getProperty("kafka.topic");

	public RDDKafkaWriter(final KafkaProducerPool pool, Boolean async) {
		this.pool = pool;
		this.async = async;
	}

	public void writeToKafka(String topic,String message) {
		KafkaProducer<String, String> producer = pool.borrowProducer();
		
		/**
		 * 把topic插入到消息的第一列中，每类topic对应一类风机
		 */
		 StringBuffer _message = new StringBuffer(topic);
		 _message.append(",").append(message);
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(
				kafka_topic, _message.toString());
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
