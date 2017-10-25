package com.macro.test.spark.streaming;

import kafka.serializer.StringDecoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.macro.test.util.ConfigurationManager;
import com.macro.test.util.JDBCUtils;

import scala.Tuple2;

import java.sql.ResultSet;
import java.util.*;

/**
 * 实时告警：
 * 1.一分钟测点数据不变
 * 2.数值超过阈值
 * 3.简单关联
 * 4.时间维度比较
 */
public class RealTimeWarn {
	
	protected static Log log = LogFactory.getLog(RealTimeWarn.class);
	
	protected static String hdfs_uri = ConfigurationManager.getProperty("hdfs.uri");
	protected static String broker_list = ConfigurationManager.getProperty("kafka.metadata.broker.list");
	protected static String kafka_topic = ConfigurationManager.getProperty("kafka.topic");
	
	public static void main(String[] args) throws Exception {
    	
		log.warn("启动实时告警处理程序");
    	
        SparkConf sparkConf = new SparkConf()/*.setMaster("local[*]")*/.setAppName("RealTimeWarn");
        final String checkpointDir = hdfs_uri + "/tmp/RealTimeWarn_checkpoint";
        
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        jssc.checkpoint(checkpointDir);
        HiveContext hiveContext = new HiveContext(jssc.sparkContext());

        // 构建kafka参数map
        // 主要要放置的就是，你要连接的kafka集群的地址（broker集群的地址列表）
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list",broker_list);
        kafkaParams.put("group.id","test_group1");
        //kafkaParams.put("auto.offset.reset","smallest");
        
        // 构建topic set
        Set<String> topics = new HashSet<String>();
        topics.add(kafka_topic);

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
        
        Row[] results = hiveContext.sql("SELECT key, value FROM src").collect();
        System.out.println(results.toString());
        
        /**
         * 告警：
         */
        //1.一分钟测点数据不变的告警
        realTimeWarn1(rowDStream);
        
        //2.阈值、关联规则、与过去某一时刻对比
        realTimeWarn2(rowDStream,hiveContext);
        
        rowDStream.print();
        
        jssc.start();
        jssc.awaitTermination();
    }

	private static void realTimeWarn2(JavaDStream<String> rowDStream,final HiveContext hiveContext) {
		rowDStream.foreachRDD(new VoidFunction2<JavaRDD<String>,Time>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(final JavaRDD<String> rdd, Time v2) throws Exception {
				rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Iterator<String> t) throws Exception {
						
						while(t.hasNext()){
							
							/**
							 * 阈值告警
							 */
							//随机生成一个阈值，大于阈值则报警
							Random random = new Random();
							int n = random.nextInt(150)+100;
							
							//String field = "TAG2"; //测点字段
							final int index = 354;
							String str = t.next();
							String[] ss = str.split(",");
							double val = Double.parseDouble(ss[index]);
							
							//大于阈值则告警，存入数据库
							if(val > n){
								String sql = "INSERT INTO record2(time,val,cal) VALUES(?,?,?)";
								
								List<Object[]> paramsList = new ArrayList<Object[]>();
								Object[] params = new Object[]{ss[1],str,n};
								paramsList.add(params);
								
								JDBCUtils jdbcUtils = JDBCUtils.getInstance();
								jdbcUtils.executeBatch(sql, paramsList);
							}
							
							
							/**
							 * 关联规则
							 */
							//boolean isAssociation = associationRule(ss[1],"","");
							
							
							/**
							 * 与过去某一时刻对比
							 */
							//String time = "2015-11-30 12:12:12";
							Row[] results = hiveContext.sql("SELECT key, value FROM src").collect();
					        System.out.println(results.toString());
						}
					}
					
					/**
					 * 关联规则
					 * @param turbineID
					 */
					private boolean associationRule(String turbineID,String field1,String field2) {
						//根据风机ID，查下风机类型ID
						String sql = "select TypeID from TABLE_S1 where TurbineID=?";
						List<Object[]> paramsList = new ArrayList<Object[]>();
						Object[] params = new Object[]{turbineID};
						paramsList.add(params);
						
						final List<Float> rpList = new ArrayList<Float>();
						final List<Float> wsList = new ArrayList<Float>();
						
						JDBCUtils jdbcUtils = JDBCUtils.getInstance();
						jdbcUtils.executeQuery(sql, params, new JDBCUtils.QueryCallback() {
							@Override
							public void process(ResultSet rs) throws Exception {
								int typeID = 0;
								while(rs.next()) {
									typeID = rs.getInt(1);
								}
								
								//查询对应风机类型的信息
								String sql = "select RP, WS from TABLE_P"+typeID;
								JDBCUtils jdbcUtils = JDBCUtils.getInstance();
								jdbcUtils.executeQuery(sql, null, new JDBCUtils.QueryCallback(){
									@Override
									public void process(ResultSet rs) throws Exception {
										while(rs.next()) {
											rpList.add(rs.getFloat(1));
											wsList.add(rs.getFloat(2));
										}
									}});
							}
						});
						return false;
					}
				});
			}
		});
	}

	private static void realTimeWarn1(JavaDStream<String> rowDStream) {
		//String field = "K5"; //测点字段
		final int index = 7;
		
		//原kafka流数据格式：<topic,yyyy-MM-dd hh:mm:ss,风机ID>
		//先将流的RDD组装成<风机ID,<测点数据|测点数据...>>
		JavaPairDStream<String, Iterable<String>> pairDStream = rowDStream.window(Durations.minutes(1), Durations.seconds(10))
			.mapToPair(
				new PairFunction<String, String, String>() {
					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<String, String> call(String str) throws Exception {
						String[] ss = str.split(",");
						//String time = ss[1].substring(0, ss[1].length()-3);
						String _str = str.replace(",", "|");
						return new Tuple2<String,String>(ss[1],_str);
					}
			}).groupByKey();
		
		//计算
		pairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String,Iterable<String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaPairRDD<String, Iterable<String>> rdd) throws Exception {
				rdd.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void call(Tuple2<String, Iterable<String>> t) throws Exception {
						Set<String> sets = new HashSet<String>();
						String value = "";
						for (String str : t._2) {
							String _str = str.replace("|", ",");
							String[] ss = _str.split(",");
							sets.add(ss[index]);
							value = _str;
						}
						
						//如果集合大小等于1，则说明集合中的数值都相同
						if(sets.size()==1){
							String sql = "INSERT INTO record(time,val) VALUES(?,?)";
							
							List<Object[]> paramsList = new ArrayList<Object[]>();
							Object[] params = new Object[]{t._1,value};
							paramsList.add(params);
							
							JDBCUtils jdbcUtils = JDBCUtils.getInstance();
							jdbcUtils.executeBatch(sql, paramsList);
						}
					}
				});
			}
		});
		
		//计算
		/*JavaPairDStream<String, Iterable<String>> pairDStream2 = pairDStream.filter(
			new Function<Tuple2<String,Iterable<String>>, Boolean>() {
				private static final long serialVersionUID = 1L;
	
				@Override
				public Boolean call(Tuple2<String, Iterable<String>> t) throws Exception {
					Set<String> sets = new HashSet<String>();
					String value = "";
					for (String str : t._2) {
						String _str = str.replace("|", ",");
						String[] ss = _str.split(",");
						sets.add(ss[index]);
						value = _str;
					}
					
					//如果集合大小等于1，则说明集合中的数值都相同
					if(sets.size()==1){
						String sql = "INSERT INTO record(time,val) VALUES(?,?)";
						
						List<Object[]> paramsList = new ArrayList<Object[]>();
						Object[] params = new Object[]{t._1,value};
						paramsList.add(params);
						
						JDBCUtils jdbcUtils = JDBCUtils.getInstance();
						jdbcUtils.executeBatch(sql, paramsList);
						return true;
					}
					return false;
				}
			});
		
		pairDStream2.print();*/
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
		//StructType schema = DataTypes.createStructType(fields);
		
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
