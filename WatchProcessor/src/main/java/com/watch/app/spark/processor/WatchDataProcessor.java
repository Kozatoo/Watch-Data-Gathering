package com.watch.app.spark.processor;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.Optional;

import com.watch.app.spark.util.WatchDataDecoder;
import com.watch.app.spark.util.PropertyFileReader;
import com.watch.app.spark.vo.WatchData;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
import scala.Tuple3;


public class WatchDataProcessor {
	
	 private static final Logger logger = Logger.getLogger(WatchDataProcessor.class);
	
	 public static void main(String[] args) throws Exception {
		 Properties prop = PropertyFileReader.readPropertyFile();

		 //read Spark  properties and create SparkConf

		 SparkConf conf = new SparkConf()
				 .setMaster(prop.getProperty("com.watch.app.spark.master"))
				 .setAppName(prop.getProperty("com.watch.app.spark.app.name"));

	//	 JavaSparkContext jsc = new JavaSparkContext(conf);






		 //interval  5s
		 JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		 //add check point directory
		 jssc.checkpoint(prop.getProperty("com.watch.app.spark.checkpoint.dir"));


		 //read  Kafka properties
		 Map<String, String> kafkaParams = new HashMap<String, String>();
		 kafkaParams.put("zookeeper.connect", prop.getProperty("com.watch.app.kafka.zookeeper"));
		 kafkaParams.put("metadata.broker.list", prop.getProperty("com.watch.app.kafka.brokerlist"));
		 String topic = prop.getProperty("com.watch.app.kafka.topic");
		 Set<String> topicsSet = new HashSet<String>();
		 topicsSet.add(topic);
		 //create kafka stream
		 JavaPairInputDStream<String, WatchData> directKafkaStream = KafkaUtils.createDirectStream(
			        jssc,
			        String.class,
			        WatchData.class,
			        StringDecoder.class,
			        WatchDataDecoder.class,
			        kafkaParams,
			        topicsSet
			    );
		 logger.info("Starting Stream Processing");

		 //DataProcessor With Functions to call and interact with Hbase
		 WatchTrafficDataProcessor watchTrafficProcessor = new WatchTrafficDataProcessor();
		 //non filterd for whole view calculations & storing them in database
		 JavaDStream<WatchData> nonFilteredWatchDataStream = directKafkaStream.map(tuple -> tuple._2());

		 watchTrafficProcessor.saveViews(nonFilteredWatchDataStream);
		 //filtered stream for total and traffic data calculation (filtered means non-redundant users)
		 //unique user per batch
		 JavaPairDStream<String, WatchData> watchDataPairStream = nonFilteredWatchDataStream.mapToPair(watch -> new Tuple2<String, WatchData>(watch.getUserId(),watch)).reduceByKey((a, b) -> a );
		
		 // Check user Id is already processed (to not count it multiple times)
		 //uninque user per hour
		 JavaMapWithStateDStream<String, WatchData, Boolean, Tuple2<WatchData,Boolean>> watchDStreamWithStatePairs = watchDataPairStream
							.mapWithState(StateSpec.function(processedUserFunc).timeout(Durations.seconds(3600)));//maintain state for one hour

		 // Filter processed user ids and keep un-processed
		 JavaDStream<Tuple2<WatchData,Boolean>> filteredWatchDStreams = watchDStreamWithStatePairs.map(tuple2 -> tuple2)
							.filter(tuple -> tuple._2.equals(Boolean.FALSE));

		 // Get stream of Watchdata
		 JavaDStream<WatchData> filteredWatchDataStream = filteredWatchDStreams.map(tuple -> tuple._1);
		 //cache stream as it is used in total and window based computation
		 filteredWatchDataStream.cache();



		 watchTrafficProcessor.processTotalTrafficData(filteredWatchDataStream);
		 watchTrafficProcessor.processWindowTrafficData(filteredWatchDataStream);

		 //start context
		 jssc.start();            
		 jssc.awaitTermination();  
  }
	 //Function to check processed users.
	private static final Function3<String, Optional<WatchData>, State<Boolean>, Tuple2<WatchData,Boolean>> processedUserFunc = (String, watch, state) -> {
			Tuple2<WatchData,Boolean> user = new Tuple2<>(watch.get(),false);
			if(state.exists()){
				user = new Tuple2<>(watch.get(),true);
			}else{
				state.update(Boolean.TRUE);
			}
			return user;
		};
          
}
