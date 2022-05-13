package com.watch.app.kafka.util;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.watch.app.kafka.vo.WatchData;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class WatchDataEncoder implements Encoder<WatchData> {

    private static final Logger logger = Logger.getLogger(WatchDataEncoder.class);
    private static ObjectMapper objectMapper = new ObjectMapper();
    public WatchDataEncoder(VerifiableProperties verifiableProperties) {

    }
    public byte[] toBytes(WatchData watchEvent) {
        try {
            String msg = objectMapper.writeValueAsString(watchEvent);
            logger.info(msg);
            return msg.getBytes();
        } catch (JsonProcessingException e) {
            logger.error("Error in Serialization", e);
        }
        return null;
    }
}
