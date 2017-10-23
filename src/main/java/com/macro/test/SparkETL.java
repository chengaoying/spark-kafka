package com.macro.test;

import kafka.serializer.StringDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import com.macro.test.util.ConfigurationManager;
import com.macro.test.util.DateUtils;
import com.macro.test.util.JDBCUtils;

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
	
	public static void main(String[] args) throws Exception {
    	
    	log.warn("启动流处理测试程序");
    	
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaToSparkStreaming");
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
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(String str) throws Exception {
					String[] ss = str.split(",");
					return new Tuple2<String,String>(ss[0],str);
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
         * 告警：
         * 1.一分钟测点数据不变
         * 2.阈值
         */
        realTimeWarn(rowDStream);
        
  		/**
         * 数据存入HDFS中
         */
  		//saveDataToHDFS(rowDStream);
  		
  		
        rowDStream.print();
        //rowDStream.dstream().saveAsTextFiles(hdfs_uri + "/tmp/data/kafka/", "kafkaData");
  		
        jssc.start();
        jssc.awaitTermination();
    }
	
	private static void realTimeWarn(JavaDStream<String> rowDStream) {
		String field = "K5"; //测点字段
		final int index = 6;
		
		//先将流的RDD组装成<yyyy-MM-dd hh:mm,<测点数据,测点数据...>>
		JavaPairDStream<String, Iterable<String>> pairDStream = rowDStream.window(Durations.minutes(10), Durations.seconds(5))
			.mapToPair(
				new PairFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<String, String> call(String str) throws Exception {
						String[] ss = str.split(",");
						String time = ss[0].substring(0, ss[0].length()-3);
						String _str = str.replace(",", "|");
						return new Tuple2<String,String>(time,_str);
					}
			}).groupByKey();
		
		JavaPairDStream<String, Iterable<String>> pairDStream2 = pairDStream.filter(
			new Function<Tuple2<String,Iterable<String>>, Boolean>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public Boolean call(Tuple2<String, Iterable<String>> t) throws Exception {
					Set<String> sets = new HashSet<String>();
					String value = "";
					for (String str : t._2) {
						String[] ss = str.split("|");
						sets.add(ss[index]);
						value = str;
					}
					
					//如果集合大小等于1，则说明集合中的数值都相同
					if(sets.size()==1){
						String _value = value.replace("|", ",");
						/*Record record = new Record();
						record.setValue(_value);*/
						
						String sql = "INSERT INTO record(val) VALUES(?)";
						
						List<Object[]> paramsList = new ArrayList<Object[]>();
						Object[] params = new Object[]{_value};
						paramsList.add(params);
						
						JDBCUtils jdbcUtils = JDBCUtils.getInstance();
						jdbcUtils.executeBatch(sql, paramsList);
						return true;
					}
					return false;
				}
			});
		
		pairDStream2.print();
	}

	private static void saveDataToHDFS(JavaDStream<String> rowDStream) {
		final String fengjiA = "01001";
		final String fengjiB = "01002";
		final String fengjiC = "01003";
		final String fengjiD = "01004";
		
		JavaDStream<String> FengJiADStream = rowDStream.filter(
				new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;
	
					@Override
					public Boolean call(String str) throws Exception {
						String[] ss = str.split(",");
						if(ss != null && ss.length > 0 && ss[1].equals(fengjiA))
							return true;
						return false;
					}
				});
		FengJiADStream.dstream().saveAsTextFiles(hdfs_uri + "/tmp/data/"+fengjiA+"/", "kafkaData");
		
		JavaDStream<String> FengJiBDStream = rowDStream.filter(
				new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String str) throws Exception {
						String[] ss = str.split(",");
						if(ss != null && ss.length > 0 && ss[1].equals(fengjiB))
							return true;
						return false;
					}
				});
		FengJiBDStream.dstream().saveAsTextFiles(hdfs_uri + "/tmp/data/"+fengjiB+"/", "kafkaData");
		
		JavaDStream<String> FengJiCDStream = rowDStream.filter(
				new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String str) throws Exception {
						String[] ss = str.split(",");
						if(ss != null && ss.length > 0 && ss[1].equals(fengjiC))
							return true;
						return false;
					}
				});
		FengJiCDStream.dstream().saveAsTextFiles(hdfs_uri + "/tmp/data/"+fengjiC+"/", "kafkaData");
		
		JavaDStream<String> FengJiDDStream = rowDStream.filter(
				new Function<String, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(String str) throws Exception {
						String[] ss = str.split(",");
						if(ss != null && ss.length > 0 && ss[1].equals(fengjiD))
							return true;
						return false;
					}
				});
		FengJiDDStream.dstream().saveAsTextFiles(hdfs_uri + "/tmp/data/"+fengjiD+"/", "kafkaData");
	}
	
	
	public static void genSparkSQLScheam(JavaDStream<String> rowDStream){
		//根据风机ID分组
		/*JavaPairDStream<String, Iterable<String>> groupDStream = rowDStream.mapToPair(
        	new PairFunction<String, String, String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, String> call(String str) throws Exception {
					String[] ss = str.split(",");
					String _str = str.replace(",", "|");
					return new Tuple2<String,String>(ss[1],_str);
				}
        	}).groupByKey();*/
		
		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName: schemaString.split(" ")) {
		  fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);
		
		// Convert records of the RDD (people) to Rows.
		/*JavaRDD<Row> rowRDD = rowDStream.foreachRDD(
		  new Function<String, Row>() {
		    public Row call(String record) throws Exception {
		      return RowFactory.create(Arrays.asList(record.split(",")));
		    }
		  });*/
		
		 // Get the lines, load to sqlContext  
		/*rowDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>() {  
            private static final long serialVersionUID = 1L;  
  
            public void call(JavaPairRDD<String, String> t) throws Exception {  
                if(t.count() < 1) return ;  
            }  
        }); */
	}

}
