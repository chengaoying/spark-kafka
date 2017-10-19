package com.macro.test;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Media.print;

/**
 * 将kafka数据导入到hive表中
 * @author jakce
 *
 */
public class KafkaToSparkStreaming {

    public static void main(String[] args) throws Exception {
    	
    	/*if (args.length < 1) {
    		System.err.println("Usage: KafkaToSparkStreaming <master>");
    		System.exit(1);
    	}*/
    	
        SparkConf sparkConf = new SparkConf().setAppName("KafkaNetworkWordCount");
        if (args.length > 0 && "local".equals(args[0])) {
    		sparkConf.setMaster("local[*]");
    	}
        //sparkConf.set("spark.streaming.backpressure.enabled", "true");
        //sparkConf.set("spark.sql.parquet.compression.codec", "snappy");
        //sparkConf.set("spark.sql.parquet.mergeSchema", "true");
        //sparkConf.set("spark.sql.parquet.binaryAsString", "true");
        
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        final HiveContext sqlContext = new HiveContext(jssc.sparkContext()); 

        // 构建kafka参数map
        // 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","192.168.0.221:9092,192.168.0.222:9092,192.168.0.223:9092");
        //kafkaParams.put("kafka.ofset.reset","0");
        
        // 构建topic set
        Set<String> topics = new HashSet<String>();
        topics.add("test_topic");

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
        			
        		}
        );
        
        logDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {  
            private static final long serialVersionUID = 1L;  
  
            public void call(JavaRDD<String> t) throws Exception {  
                if(t.count() < 1) return ;  
                DataFrame df = sqlContext.read().json(t);  
                df.show();  
            }  
        });  
        
        //logDStream.print();
        
        //logDStream.dstream().saveAsTextFiles("hdfs://192.168.0.224:8020/user/hive/data/", "kafkaData");
        
        // 创建JavaSparkContext
     	//JavaSparkContext sc = new JavaSparkContext(sparkConf);
     	// 创建HiveContext
     	//final HiveContext hiveContext = new HiveContext(jssc.sc().sc());
     	
     	//遍历
     	/*logDStream.foreachRDD(
     			new Function<JavaRDD<String>, Void>() {
					private static final long serialVersionUID = -6208175829524293079L;

					@Override
					public Void call(JavaRDD<String> str) throws Exception {
						String s = "INSERT INTO table1 VALUES(\"ss\",\"zz\")";
						hiveContext.sql(s);
						return null;
					}
     				
		});*/
        

        jssc.start();
        jssc.awaitTermination();
    }

}
