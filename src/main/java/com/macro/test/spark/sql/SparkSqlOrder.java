package com.macro.test.spark.sql;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import com.macro.test.spark.streaming.SparkETL;


public class SparkSqlOrder {
	
	protected static Log log = LogFactory.getLog(SparkETL.class);
	
    public static void main(String[] args) throws Exception {
    	log.warn("启动Spark排序处理程序");
    	
        SparkConf sparkConf = new SparkConf().setAppName("SparkSqlOrder");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(ctx.sc());
        
        hiveContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"");
        hiveContext.sql("LOAD DATA INPATH '/tmp/files/kv1.txt' INTO TABLE src");
        Row[] results = hiveContext.sql("SELECT key, value FROM src").collect();

        System.out.println(results.length);
    }
}