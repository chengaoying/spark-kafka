package com.macro.test.spark.streaming;

import kafka.serializer.StringDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.macro.test.kafka.JavaDStreamKafkaWriter;
import com.macro.test.util.ConfigurationManager;
import com.macro.test.util.DateUtils;

import scala.Tuple2;

import java.util.*;

/**
 * 数据清洗：使用sparkstreaming从kafka实时接入数据，然后对数据进行清洗
 * 符合标准UTC、去重，存入hdfs
 * @author jakce
 *
 */
public class SparkETL {
	
	protected static Log log = LogFactory.getLog(SparkETL.class);
	
	protected static String hdfs_uri = ConfigurationManager.getProperty("hdfs.uri");
	protected static String broker_list = ConfigurationManager.getProperty("kafka.metadata.broker.list");
	protected static String kafka_topics = ConfigurationManager.getProperty("kafka.topics");
	protected static String start_time = ConfigurationManager.getProperty("kafka.startTime");
	protected static String end_time = ConfigurationManager.getProperty("kafka.endTime");
	
	//protected static String topic = "test_topic";
	
	public static void main(String[] args) throws Exception {
    	
    	log.warn("启动Spark ETL处理程序");
    	
    	if(args.length < 1){
    		System.err.println("Usage: SparkETL <topic>");
    	    System.exit(1);
    	}
    	String topic = args[0];
    	
        SparkConf sparkConf = new SparkConf()/*.setMaster("local[*]")*/.setAppName("SparkETL");
        final String checkpointDir = hdfs_uri + "/tmp/SparkETL_checkpoint";
        
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        jssc.checkpoint(checkpointDir);
        
        //final Accumulator<Integer> count = jssc.sparkContext().accumulator(0, "接收的kafka记录条数");

        // 构建kafka参数map
        // 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",broker_list);
        kafkaParams.put("group.id","test_group1");
        kafkaParams.put("auto.offset.reset","smallest");
        
        // 构建topic set
        Set<String> topics = new HashSet<String>();
        topics.add(topic);
        /*String[] _topics = kafka_topics.split(",");
        for (String str : _topics) {
        	topics.add(str);
		}*/

        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairInputDStream<String, String> kafkaDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
        
        //将一条日志拆分诚多条（分隔符为";"）
        JavaDStream<String> logDStream = kafkaDStream.flatMap(
			new FlatMapFunction<Tuple2<String,String>, String>() {
				private static final long serialVersionUID = -4327021536484339309L;
			
				@Override
				public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {
					return Arrays.asList(tuple2._2.split(";"));
				}
			});
        
        
        /** 数据清洗：
         * 	1.过滤时间不符合标准UTC时间
         */
        JavaDStream<String> filterLogDStream = logDStream.filter(
			new Function<String, Boolean>() {
				private static final long serialVersionUID = -3752279481282155425L;
				
				@Override
				public Boolean call(String str) throws Exception {
					String[] ss = str.split(",");
					if(ss != null && ss.length > 1 
							&& DateUtils.isValidDate(ss[0]))
							//&& DateUtils.isInTimePeriod(ss[0], start_time, end_time))
						return true;
					else
						return false;
				}
			});

        /** 数据清洗：
         * 	2.去重：首先转换诚JavaPairDStream，然后使用reduceBykey去重
         */
        JavaPairDStream<String,String> distinctDStream = filterLogDStream.mapToPair(
        	new PairFunction<String, String, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(String str) throws Exception {
					String[] ss = str.split(",");
					return new Tuple2<String,String>(ss[0]+"_"+ss[1],str);
				}
        	}).reduceByKey(
        	new Function2<String, String, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String call(String v1, String v2) throws Exception {
					return v1;
				}
			});
        
        //将JavaPairDStream转换成JavaDStream
        JavaDStream<String> rowDStream = distinctDStream.map(
        	new Function<Tuple2<String,String>, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public String call(Tuple2<String, String> t) throws Exception {
					return t._2;
				}
        	});
        
        //rowDStream.cache();
        
        /**
         * 数据推送到Kafka流中
         */
        saveDataToKafka(rowDStream,topic);
        
        /**
         * 数据存入HDFS中
         */
        //rowDStream.print();
        rowDStream.dstream().saveAsTextFiles(hdfs_uri + "/tmp/data/" + topic + "/", "kafkaData");
        
        jssc.start();
        jssc.awaitTermination();
    }

	private static void saveDataToKafka(JavaDStream<String> rowDStream,String topic) {
		new JavaDStreamKafkaWriter(rowDStream,topic,true).writeToKafka();
	}

}
