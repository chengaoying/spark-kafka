package com.macro.test.spark.streaming;

import kafka.serializer.StringDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;
import com.macro.test.util.ConfigurationManager;
import com.macro.test.util.JDBCUtils;

import scala.Tuple2;

import java.util.*;

/**
 * 实时统计分析：
 * 1.统计KPI指标：相加求和
 * 2.实时数据统计
 */
public class RealTimeAnalysis {
	
	protected static Log log = LogFactory.getLog(RealTimeAnalysis.class);
	
	protected static String hdfs_uri = ConfigurationManager.getProperty("hdfs.uri");
	protected static String broker_list = ConfigurationManager.getProperty("kafka.metadata.broker.list");
	protected static String kafka_topics = ConfigurationManager.getProperty("kafka.topics");
	
	protected static String topic = "topic_1";
	
	protected static String typeA = "lhdl15";
	protected static String typeB = "lhdl20";
	protected static String typeC = "my15";
	protected static String typeD = "rest";
	
	public static void main(String[] args) throws Exception {
    	
		log.warn("启动实时数据统计分析程序");
    	
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeAnalysis");
        final String checkpointDir = hdfs_uri + "/tmp/RealTimeAnalysis_checkpoint";
        
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        jssc.checkpoint(checkpointDir);

        // 构建kafka参数map
        // 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",broker_list);
        kafkaParams.put("group.id","test_group1");
        //kafkaParams.put("auto.offset.reset","smallest");
        
        // 构建topic set
        Set<String> topics = new HashSet<String>();
        topics.add(topic);

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
        
        //将kafka流数据<key,value>转换成<value>格式
        JavaDStream<String> rowDStream = realTimeLogDStream.map(
        		new Function<Tuple2<String,String>, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public String call(Tuple2<String, String> tuple) throws Exception {
						// TODO Auto-generated method stub
						return tuple._2;
					}
        		});
        
        //将kafka流数据<null,value>转换成<topic,value>格式
        /*JavaPairDStream<String, String> pairDStream = realTimeLogDStream.mapToPair(
        		new PairFunction<Tuple2<String,String>, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<String, String> t) throws Exception {
						String[] ss = t._2.split(",");
						return new Tuple2<String,String>(ss[0],t._2);
					}
				});*/
        
        /**
         * 分析：
         */
        //1.实时统计分析
        realTimeAnalysis1(rowDStream,typeA,"TAG2",506);//风机类型A
        
        //2.实时数据落地统计
        realTimeAnalysis2(rowDStream,typeA,"TAG2",506);
        
        rowDStream.print();
        
        jssc.start();
        jssc.awaitTermination();
    }

	/**
	 * 实时分析
	 * @param rowDStream 分析流
	 * @param type 风机类型
	 * @param field 测点数据
	 * @param index 测点数据在String字符串中的index位置
	 */
	private static void realTimeAnalysis1(JavaDStream<String> rowDStream, final String type,
			final String field, final int index) {
		JavaDStream<String> fengjiADStream = rowDStream.filter(
				new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String str) throws Exception {
						String[] ss = str.split(",");
						if(ss[0].equals(topic))
							return true;
						return false;
					}
			});
		
		//将数据格式转换为<测点字段，value>
		JavaPairDStream<String, Double> fengjiAAvgDStream = fengjiADStream.mapToPair(
				new PairFunction<String, String, Double>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Double> call(String t) throws Exception {
						String[] ss = t.split(",");
						return new Tuple2<String,Double>(String.valueOf(index),Double.parseDouble(ss[index]));
					}
			});
		
		//计算平均值，使用updateStateByKey计算全局平均值
		JavaPairDStream<String, Double> avgDStream = fengjiAAvgDStream.updateStateByKey(
				new Function2<List<Double>, Optional<Double>, Optional<Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Optional<Double> call(List<Double> values, Optional<Double> state) throws Exception {
						//获取之前的平均值，初始值为0.0
						Double avg = 0.0;
						if(state.isPresent()) {
							avg = state.get();
						}
						
						//统计这个batch的平均值
						Double count = 0.0;
						for (Double double1 : values) {
							count += double1;
						}
						Double _avg = count/values.size();
						
						//如果avg为0.0，则直接返回这次计算的平均
						if(avg == 0.0){
							return Optional.of(_avg);
						}else{
							Double new_avg = (avg + _avg) / 2;
							return Optional.of(new_avg);
						}
					}
			});
		
		//将统计数据存入数据库
		avgDStream.foreachRDD(new VoidFunction2<JavaPairRDD<String, Double>,Time>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Double> rdd, Time v2) throws Exception {
				rdd.foreach(new VoidFunction<Tuple2<String,Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Double> t) throws Exception {
						String sql = "INSERT INTO analysis1(type,field,val) VALUES(?,?,?)";
						
						List<Object[]> paramsList = new ArrayList<Object[]>();
						Object[] params = new Object[]{type,field,t._2};
						paramsList.add(params);
						
						JDBCUtils jdbcUtils = JDBCUtils.getInstance();
						jdbcUtils.executeBatch(sql, paramsList);
					}
				});
			}
		});
		avgDStream.print();
	}

	/**
	 * 实时统计：实时统计数据总数
	 * 条件：(TAGn > e)
	 * @param rowDStream 数据流
	 * @param type 风机类型
	 * @param field 测点数据
	 * @param index 测点数据在String字符串中的index位置
	 */
	private static void realTimeAnalysis2(JavaDStream<String> rowDStream, final String type,
			final String field, final int index) {
		//过滤（TAGn > e）的记录
		final double e = 130.2;
		JavaDStream<String> filterDStream = rowDStream.filter(
				new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String str) throws Exception {
						String[] ss = str.split(",");
						double val = Double.parseDouble(ss[index]);
						if(val > e)
							return true;
						return false;
					}
			});
		
		//1.先将记录格式<time,ID,...TAGn,>转换成<type_TAGn,1>格式
		//2.使用updateStateByKey进行全局统计
		JavaPairDStream<String,Integer> countDStream = filterDStream.mapToPair(
				new PairFunction<String, String,Integer>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> call(String str) throws Exception {
						return new Tuple2<String, Integer>(type+"_"+field,1);
					}
			}).updateStateByKey(
				new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
						// 首先定义一个全局的记录计数
						Integer newValue = 0;
						
						// 其次，判断，state是否存在，如果不存在，说明是一个key第一次出现
						// 如果存在，说明这个key之前已经统计过全局的次数了
						if(state.isPresent()) {
							newValue = state.get();
						}
						
						// 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计
						// 次数
						for(Integer value : values) {
							newValue += value;
						}
						
						return Optional.of(newValue);  
					}
			});
		
		//将统计数据存入数据库
		countDStream.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>,Time>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Integer> rdd, Time v2) throws Exception {
				rdd.foreach(new VoidFunction<Tuple2<String,Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Integer> t) throws Exception {
						String sql = "INSERT INTO analysis2(type,field,val) VALUES(?,?,?)";
						
						List<Object[]> paramsList = new ArrayList<Object[]>();
						Object[] params = new Object[]{type,field,t._2};
						paramsList.add(params);
						
						JDBCUtils jdbcUtils = JDBCUtils.getInstance();
						jdbcUtils.executeBatch(sql, paramsList);
					}
				});
			}
		});
	}
	
	public String getFengjiType(String type){
		if(type.equals(typeA)){
			return "A类风机";
		}else if(type.equals(typeB)){
			return "B类风机";
		}else if(type.equals(typeC)){
			return "C类风机";
		}else if(type.equals(typeD)){
			return "D类风机";
		}
		return null;
	}

}
