package com.watch.app.spark.processor;

import com.watch.app.spark.vo.WatchData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import com.watch.app.spark.vo.WatchData;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Date;


public class WatchBatchProcessor {

    public void createHbaseTable() {

        Configuration config = HBaseConfiguration.create();
        SparkConf sparkConf = new SparkConf().setAppName("SparkBatchProcessor").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        config.set(TableInputFormat.INPUT_TABLE,"view");

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                    jsc.newAPIHadoopRDD(config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        // in the rowPairRDD the key is hbase's row key, The Row is the hbase's Row data
        JavaPairRDD<String, WatchData> rowPairRDD = hBaseRDD.mapToPair(
                new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, WatchData>() {
                    @Override
                    public Tuple2<String, WatchData> call(
                            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

                        Result r = entry._2;
                        String keyRow = Bytes.toString(r.getRow());
                        String userId = keyRow.split(";")[0];
                        Date timestamp = new Date(keyRow.split(";")[1]);
                        // define java bean
                        WatchData cd = new WatchData(userId,"male",
                                (String) Bytes.toString(r.getValue(Bytes.toBytes("watchedEpisode"), Bytes.toBytes("ShowId"))),
                                (String) Bytes.toString(r.getValue(Bytes.toBytes("watchEpisode"), Bytes.toBytes("Episode"))),
                                (String) Bytes.toString(r.getValue(Bytes.toBytes("watchEpisode"), Bytes.toBytes("Season"))),
                                timestamp,
                                (Double)Double.parseDouble(( Bytes.toString(r.getValue(Bytes.toBytes("watchDetail"), Bytes.toBytes("rating"))))),
                                (Double) Double.parseDouble(Bytes.toString(r.getValue(Bytes.toBytes("watchDetail"), Bytes.toBytes("time")))));
                       return new Tuple2<String, WatchData>(keyRow, cd);
                    }
                });
        JavaPairRDD<String,Integer> finalResult =rowPairRDD.map(data -> data._2)
                .mapToPair(data->new Tuple2<String,Integer>(data.getUserId()+data.getShowId(),1))
                .reduceByKey((a,b)->a+b);
        finalResult.saveAsTextFile("batchResult.txt");
        finalResult.foreach((data)->{
            Configuration configs = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(configs);
            Admin admin = connection.getAdmin();

            Table table1 = connection.getTable(TableName.valueOf("batch"));
                try{
                    Put put= new Put(Bytes.toBytes(data._1));
                    put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("count"),Bytes.toBytes(data._2.toString()));
                    table1.put(put);
                }
                catch (Exception e){
                    e.printStackTrace();
                }

            table1.close();
            connection.close();
        });

        System.out.println(finalResult);
        System.out.println("nombre d'enregistrements: "+finalResult.count());

    }

    public static void main(String[] args){
        WatchBatchProcessor admin = new WatchBatchProcessor();
        admin.createHbaseTable();
    }

}
