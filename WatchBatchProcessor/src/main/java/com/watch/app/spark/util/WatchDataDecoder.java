package com.watch.app.spark.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.watch.app.spark.vo.WatchData;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;


public class WatchDataDecoder implements Decoder<WatchData> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public WatchDataDecoder(VerifiableProperties verifiableProperties) {

    }
    public WatchData fromBytes(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, WatchData.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
