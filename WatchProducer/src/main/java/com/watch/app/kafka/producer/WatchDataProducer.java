package com.watch.app.kafka.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.watch.app.kafka.util.PropertyFileReader;
import com.watch.app.kafka.vo.WatchData;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class WatchDataProducer {

    private static final Logger logger = Logger.getLogger(WatchDataProducer.class);

    public static void main(String[] args) throws Exception {
        //read config file
        Properties prop = PropertyFileReader.readPropertyFile();
        String zookeeper = prop.getProperty("com.watch.app.kafka.zookeeper");
        String brokerList = prop.getProperty("com.watch.app.kafka.brokerlist");
        String topic = prop.getProperty("com.watch.app.kafka.topic");
        logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);

        // set producer properties
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeeper);
        properties.put("metadata.broker.list", brokerList);
        properties.put("request.required.acks", "1");
        properties.put("serializer.class", "com.watch.app.kafka.util.WatchDataEncoder");
        //generate event
        Producer<String, WatchData> producer = new Producer<String, WatchData>(new ProducerConfig(properties));
        WatchDataProducer watchProducer = new WatchDataProducer();
        watchProducer.generateWatchEvent(producer,topic);
    }



    private void generateWatchEvent(Producer<String, WatchData> producer, String topic) throws InterruptedException {
        List<String> showList = Arrays.asList(new String[]{"Game Of Thrones", "Breaking Bad", "Black Mirror","Better Call Saul","Dexter","Bojack Horseman","Mr Robot"});
        List<String> userGenderList = Arrays.asList(new String[]{"Male", "Female"});
        Random rand = new Random();
        logger.info("Sending events");
        // generate event in loop
        while (true) {
            List<WatchData> eventList = new ArrayList<WatchData>();
            for (int i = 0; i <50; i++) {// create 100 users
                String userId = UUID.randomUUID().toString();
                String userGender = userGenderList.get(rand.nextInt(2));
                String showId = showList.get(rand.nextInt(7));
                Date now = new Date();
                long unixTime = (long) ( System.currentTimeMillis()-rand.nextInt(60*60*24*10)*1000);
                Date timestamp = new Date(unixTime);
                double rating = rand.nextInt(50);
                rating = rating/10;
                // random rating between 0.1 and 5
                double episodeLength = rand.nextInt(60 - 30) + 30; //random between 30 and 60 minutes
                for (int j = 0; j < 30; j++) {// Add 5 events for each user
                    String episode = String.valueOf(rand.nextInt(12));
                    String season =  String.valueOf(rand.nextInt(2));
                    WatchData event = new WatchData(userId, userGender, showId, episode, season, timestamp, rating,episodeLength);
                    eventList.add(event);
                }
            }
            Collections.shuffle(eventList);// shuffle for random events
            for (WatchData event : eventList) {
                KeyedMessage<String, WatchData> data = new KeyedMessage<String, WatchData>(topic, event);
                producer.send(data);
                Thread.sleep(rand.nextInt(1500 - 1000));//random delay of 0.5 seconds
            }
        }
    }


}
