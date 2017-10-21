package com.macro.test;

import kafka.serializer.StringDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.macro.test.util.ConfigurationManager;
import com.macro.test.util.DateUtils;

import scala.Tuple2;

import java.util.*;

/**
 * 将kafka数据导入到hive表中
 * @author jakce
 *
 */
public class KafkaToSparkStreaming {
	
	protected static Log log = LogFactory.getLog(KafkaToSparkStreaming.class);
	
    public static void main(String[] args) throws Exception {
    	
    	log.warn("启动流处理测试程序");
    	/*if (args.length < 3) {
    		System.err.println("Usage: KafkaToSparkStreaming <hdfs_uri> <broker_list> <topic1,topic2>");
    		System.exit(1);
    	}
    	String hdfs_uri = args[0];
    	String broker_list = args[1];
    	String topic = args[2];
    	*/
    	
    	String hdfs_uri = ConfigurationManager.getProperty("hdfs.uri");
    	String broker_list = ConfigurationManager.getProperty("kafka.metadata.broker.list");
    	String kafka_topics = ConfigurationManager.getProperty("kafka.topics");
    	final String start_time = ConfigurationManager.getProperty("kafka.startTime");
    	final String end_time = ConfigurationManager.getProperty("kafka.endTime");
    	
        SparkConf sparkConf = new SparkConf().setAppName("KafkaToSparkStreaming");
        final String checkpointDir = hdfs_uri + "/tmp/streaming_checkpoint";
        
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
        String[] _topics = kafka_topics.split(",");
        for (String str : _topics) {
        	topics.add(str);
		}

        // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
        // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairInputDStream<String, String> realTimeLogDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);
        
        //将一条日志拆分诚多条（分隔符为";"）
        JavaDStream<String> logDStream = realTimeLogDStream.flatMap(
			new FlatMapFunction<Tuple2<String,String>, String>() {
				private static final long serialVersionUID = -4327021536484339309L;
			
				@Override
				public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {
					return Arrays.asList(tuple2._2.split(";"));
				}
			});
        
        
        /** 数据清洗：
         * 	1.过滤时间不符合标准UTC时间
         *  2.条件过滤：time>2015-11-30 11:59:59 && time<2015-11-30 14:00:00
         */
        JavaDStream<String> filterLogDStream = logDStream.filter(
			new Function<String, Boolean>() {
				private static final long serialVersionUID = -3752279481282155425L;
				
				@Override
				public Boolean call(String str) throws Exception {
					String[] ss = str.split(",");
					if(ss != null && ss.length > 1 
							&& DateUtils.isValidDate(ss[0])
							&& DateUtils.isInTimePeriod(ss[0], start_time, end_time))
						return true;
					else
						return false;
				}
			});
        
        
        /** 数据清洗：
         * 	3.去重：首先转换诚JavaPairDStream，然后使用reduceBykey去重
         */
        JavaPairDStream<String,String> distinctDStream = filterLogDStream.mapToPair(
        	new PairFunction<String, String, String>() {
				private static final long serialVersionUID = 171084037290528597L;

				@Override
				public Tuple2<String, String> call(String str) throws Exception {
					String[] ss = str.split(",");
					return new Tuple2<String,String>(ss[0],str);
				}
        	}).reduceByKey(
        	new Function2<String, String, String>() {
				private static final long serialVersionUID = 4693085980956592919L;

				@Override
				public String call(String v1, String v2) throws Exception {
					return v1;
				}
			});
        
        
        distinctDStream.print();
        //filterLogDStream.dstream().saveAsTextFiles(hdfs_uri + "/tmp/data/kafka/", "kafkaData");
        
        jssc.start();
        jssc.awaitTermination();
    }

}
