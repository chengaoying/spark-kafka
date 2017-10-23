package com.macro.test.kafka;

import java.io.Serializable;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.spark.streaming.api.java.JavaDStream;
import com.macro.test.util.ConfigurationManager;

public class JavaDStreamKafkaWriter implements Serializable {

	private static final long serialVersionUID = 3934800110130868334L;
	private final KafkaProducerPool pool;
	private final JavaDStream<String> javaDStream;
	private final String topic;
	private final Boolean kafkaAsync;

	public JavaDStreamKafkaWriter(
			final JavaDStream<String> javaDStream, final String topic, final Boolean kafkaAsync) {
		this.javaDStream = javaDStream;
		this.topic = topic;
		this.kafkaAsync = kafkaAsync;
		
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigurationManager.getProperty("kafka.metadata.broker.list"));//格式：host1:port1,host2:port2,....
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);//a batch size of zero will disable batching entirely
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);//send message without delay
		properties.put(ProducerConfig.ACKS_CONFIG, "1");//对应partition的leader写到本地后即返回成功。极端情况下，可能导致失败
		properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		pool = new KafkaProducerPool(3, properties);
	}

	public void writeToKafka() {
		// String tempTopic=topic.concat(UUID.randomUUID().toString());
		String tempTopic = topic;
		javaDStream.foreachRDD(new JavaRDDKafkaWriter(pool, tempTopic,kafkaAsync));
	}
}
