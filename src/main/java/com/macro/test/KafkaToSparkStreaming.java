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
import org.apache.spark.api.java.function.PairFlatMapFunction;
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
       /* JavaPairDStream<String,String> pairDStream = realTimeLogDStream.mapToPair(
    		new PairFunction<Tuple2<String,String>, String, String>() {
				@Override
				public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
					String[] ss = t._2.split(";");
					return new Tuple2<String,String>(ss[0],t._2);
				}
			}
        );*/
        
        JavaPairDStream<String,String> pairDStream = realTimeLogDStream.flatMapToPair(
        		new PairFlatMapFunction<Tuple2<String,String>, String, String>() {
					private static final long serialVersionUID = -6814161825885679045L;

					@Override
					public Iterable<Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
						String[] ss = t._2.split(";");
						List<Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>();
						for (String str : ss) {
							String[] s = str.split(",");
							Tuple2<String,String> tuple = new Tuple2<String,String>(s[0],str);
							list.add(tuple);
						}
						return list;
					}
				}
        	);
        		
        		
        /*realTimeLogDStream.flatMap(
        		new FlatMapFunction<Tuple2<String,String>, String>() {
					private static final long serialVersionUID = -4327021536484339309L;

					@Override
					public Iterable<String> call(Tuple2<String, String> tuple2) throws Exception {
						return Arrays.asList(tuple2._2.split(";"));
					}
        		}
        	)
        ;*/
        pairDStream.print();
        
        /** 数据清洗：
         * 	1.条件过滤：time>2015-11-30 11:59:59 && time<2015-11-30 14:00:00
         *  2.过滤时间不符合标准UTC时间
         */
       /* JavaDStream<String> filterLogDStream = logDStream.filter(
        		new Function<String, Boolean>() {
					private static final long serialVersionUID = -3752279481282155425L;
					
					@Override
					public Boolean call(String str) throws Exception {
						String[] ss = str.split(",");
						if(ss.length > 1 && DateUtils.isInTimePeriod(ss[0], start_time, end_time)){
							//判断是否符合标准UTC时间标准
							//TODO
							return true;
						}
						return false;
					}
				}
        );*/
        
        //去重
        //filterLogDStream.transformToPair(new Function<JavaRDD<String>,JavaPairRDD<String,String>>())
        		
        
        
        log.warn("---数据保存至HDFS---");
        //filterLogDStream.print();
        //filterLogDStream.dstream().saveAsTextFiles(hdfs_uri + "/tmp/data/kafka/", "kafkaData");
        
        jssc.start();
        jssc.awaitTermination();
    }

}
