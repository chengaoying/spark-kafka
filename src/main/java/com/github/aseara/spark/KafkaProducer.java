package com.github.aseara.spark;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;  
import kafka.producer.KeyedMessage;  
import kafka.producer.ProducerConfig;  
  
public class KafkaProducer{  
      
    private final Producer<String, String> producer;  
    public final static String TOPIC = "test_topic";
  
    private KafkaProducer(){  
          
        Properties props = new Properties();  
          
        // 此处配置的是kafka的broker地址:端口列表  
        props.put("metadata.broker.list", "192.168.0.221:9092,192.168.0.222:9092,192.168.0.223:9092");
  
        //配置value的序列化类  
        props.put("serializer.class", "kafka.serializer.StringEncoder");  
          
        //配置key的序列化类  
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");  
  
        //request.required.acks  
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).  
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).  
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.  
        props.put("request.required.acks","-1");  
  
        producer = new Producer<String, String>(new ProducerConfig(props));  
    }  
  
    void produce() {  
        int messageNo = 1;  
        final int COUNT = 1001;  
  
        int messageCount = 0;
        Random random = new Random();
        while (messageNo < COUNT) {  
            String key = String.valueOf(messageNo);
            int n = random.nextInt(10);
            int m = random.nextInt(59);
            String data = "2015-11-30 12:0"+n+":"+m+",130,1,2,3,2,"+m+",123.2123,123.2124,123.2125,123.2126,123.2127";
            producer.send(new KeyedMessage<String, String>(TOPIC, key ,data));  
            System.out.println(data);  
            messageNo ++;  
            messageCount++;  
        }  
          
        System.out.println("Producer端一共产生了" + messageCount + "条消息！");  
    }  
  
    public static void main( String[] args )  
    {  
        new KafkaProducer().produce();  
    }  
}  