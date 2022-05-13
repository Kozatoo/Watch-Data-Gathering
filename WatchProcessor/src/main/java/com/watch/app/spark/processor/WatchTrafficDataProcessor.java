package com.watch.app.spark.processor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.watch.app.spark.entity.TotalTrafficData;
import com.watch.app.spark.entity.WindowTrafficData;
import com.watch.app.spark.vo.AggregateKey;
import com.watch.app.spark.vo.WatchData;

import scala.Tuple2;

public class WatchTrafficDataProcessor {
    private static final Logger logger = Logger.getLogger(WatchTrafficDataProcessor.class);

    public void saveViews(JavaDStream<WatchData> nonFilteredWatchDataStream){
        nonFilteredWatchDataStream.foreachRDD(
                rdd->{


                    Configuration config = HBaseConfiguration.create();
                    Connection connection = ConnectionFactory.createConnection(config);
                    Admin admin = connection.getAdmin();

                    Table table1 = connection.getTable(TableName.valueOf("view"));
                    rdd.collect().forEach((data)->{
                        try{

                            Date timestamp = new Date();
                            Put put= new Put(Bytes.toBytes(data.getUserId()+";"+timestamp.toString()));
                            put.addColumn("watchedEpisode".getBytes(),Bytes.toBytes("Season"),Bytes.toBytes(data.getSeason()));
                            put.addColumn(Bytes.toBytes("watchedEpisode"),Bytes.toBytes("Episode"),Bytes.toBytes(data.getEpisode()));
                            put.addColumn(Bytes.toBytes("watchedEpisode"),Bytes.toBytes("ShowId"),Bytes.toBytes(data.getShowId()));

                            put.addColumn(Bytes.toBytes("watchDetails"),Bytes.toBytes("rating"),Bytes.toBytes(data.getRating()+""));
                            put.addColumn(Bytes.toBytes("watchDetails"),Bytes.toBytes("time"),Bytes.toBytes(data.getEpisodeLength()+""));

                            put.addColumn(Bytes.toBytes("userDetails"),Bytes.toBytes("userId"),Bytes.toBytes(data.getUserId()+""));
                            put.addColumn(Bytes.toBytes("userDetails"),Bytes.toBytes("userGender"),Bytes.toBytes(data.getUserGender()+""));
                            put.addColumn(Bytes.toBytes("userDetails"),Bytes.toBytes("watchTime"),Bytes.toBytes(data.getTimestamp()+""));


                            table1.put(put);
                        }
                        catch (Exception e){
                            e.printStackTrace();
                        }
                    });
                    table1.close();
                    connection.close();
                }
        );
    }
    /**
     * Method to get total traffic counts of different type of users for each show.
     *
     * @param filteredWatchDataStream Watch data stream
     */
    public void processTotalTrafficData(JavaDStream<WatchData> filteredWatchDataStream) {

        // We need to get count of user group by userId and showId

        JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredWatchDataStream
                .mapToPair(watch -> new Tuple2<>(new AggregateKey(watch.getUserId(),watch.getShowId()), 1L))
                .reduceByKey((a, b) -> a + b);

        // Need to keep state for total count
        JavaMapWithStateDStream<AggregateKey, Long, Long, Tuple2<AggregateKey, Long>> countDStreamWithStatePair = countDStreamPair
                .mapWithState(StateSpec.function(totalSumFunc).timeout(Durations.seconds(3600)));//maintain state for one hour

        // Transform to dstream of TrafficData
        JavaDStream<Tuple2<AggregateKey, Long>> countDStream = countDStreamWithStatePair.map(tuple2 -> tuple2);
        JavaDStream<TotalTrafficData> trafficDStream = countDStream.map(totalTrafficDataFunc);

        // save to hbase
        trafficDStream.foreachRDD(rdd->{
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();

            Table table1 = connection.getTable(TableName.valueOf("total_traffic"));
            rdd.collect().forEach   ((data)->{
                try{
                    Put put= new Put(Bytes.toBytes(data.getUserId()));
                    put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("totalCount"),Bytes.toBytes(data.getTotalCount()+""));
                    put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("recordDate"),Bytes.toBytes(data.getRecordDate()));
                    put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("TimeStamp"),Bytes.toBytes(data.getTimeStamp()+""));


                    table1.put(put);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            });
            table1.close();
            connection.close();
        });
    }

    /**
     * Method to get window traffic counts of different type of users for each route.
     * Window duration = 30 seconds and Slide interval = 10 seconds
     *
     * @param filteredWatchDataStream Watch data stream
     */
    public void processWindowTrafficData(JavaDStream<WatchData> filteredWatchDataStream) {

        // reduce by key and window (30 sec window and 10 sec slide).
        JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredWatchDataStream
                .mapToPair(watch -> new Tuple2<>(new AggregateKey(watch.getUserId(), watch.getShowId()), 1L))
                .reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(150), Durations.seconds(30));

        // Transform to dstream of TrafficData
        JavaDStream<WindowTrafficData> trafficDStream = countDStreamPair.map(windowTrafficDataFunc);

        // save to db
        trafficDStream.foreachRDD(rdd->{
            Configuration config = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();

            Table table1 = connection.getTable(TableName.valueOf("window_traffic"));
            rdd.collect().forEach   ((data)->{
                try{
                    Put put= new Put(Bytes.toBytes(data.getUserId()+"=>"+data.getShowId()));
                    put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("totalCount"),Bytes.toBytes(data.getTotalCount()+""));
                    put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("recordDate"),Bytes.toBytes(data.getRecordDate()));
                    put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("TimeStamp"),Bytes.toBytes(data.getTimeStamp()+""));


                    table1.put(put);
                }
                catch (Exception e){
                    e.printStackTrace();
                }
            });
            table1.close();
            connection.close();
        });
        Map<String, String> columnNameMappings = new HashMap<String, String>();
        columnNameMappings.put("userId", "userId");
        columnNameMappings.put("userGender", "userGender");
        columnNameMappings.put("totalCount", "totalcount");
        columnNameMappings.put("timeStamp", "timestamp");
        columnNameMappings.put("recordDate", "recorddate");

        // call CassandraStreamingJavaUtil function to save in DB
    }


    //Function to get running sum by maintaining the state
    private static final Function3<AggregateKey, Optional<Long>, State<Long>,Tuple2<AggregateKey,Long>> totalSumFunc = (key,currentSum,state) -> {
        long totalSum = currentSum.or(0L) + (state.exists() ? state.get() : 0);
        Tuple2<AggregateKey, Long> total = new Tuple2<>(key, totalSum);
        state.update(totalSum);
        return total;
    };

    //Function to create TotalTrafficData object from Watch data
    private static final Function<Tuple2<AggregateKey, Long>, TotalTrafficData> totalTrafficDataFunc = (tuple -> {
        logger.debug("Total Count : " + "key " + tuple._1().getUserId() + "-" + tuple._1().getShowId() + " value "+ tuple._2());
        TotalTrafficData trafficData = new TotalTrafficData();
        trafficData.setUserId(tuple._1().getUserId());
        trafficData.setShowId(tuple._1().getShowId());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    });

    //Function to create WindowTrafficData object from Watch data
    private static final Function<Tuple2<AggregateKey, Long>, WindowTrafficData> windowTrafficDataFunc = (tuple -> {
        logger.debug("Window Count : " + "key " + tuple._1().getUserId() + "-" + tuple._1().getShowId()+ " value " + tuple._2());
        WindowTrafficData trafficData = new WindowTrafficData();
        trafficData.setUserId(tuple._1().getUserId());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setShowId(tuple._1().getShowId());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    });



}
