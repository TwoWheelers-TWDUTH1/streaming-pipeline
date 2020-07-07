package com.tw.models;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

public class MessageMetadataTest {

    @Test
    public void shouldReturnStringWithMetadataAndPayload() {
        ZonedDateTime zonedDateTime = LocalDateTime.parse("2017-01-01T00:00:00").atZone(TimeZone.getTimeZone("Bangkok/Thailand").toZoneId());
        long ingestionTime = zonedDateTime.toInstant().toEpochMilli();
        String producerId = "Producer-id";
        String messageUUID = "123e4567-e89b-12d3-a456-426655440000";
        long size = 12;

        MessageMetadata metadata = new MessageMetadata(ingestionTime, producerId, messageUUID, size);

        String expected = "{\"producer_id\": \"Producer-id\", " +
                "\"size\": 12, " +
                "\"message_id\": \"123e4567-e89b-12d3-a456-426655440000\", " +
                "\"ingestion_time\": 1483228800000}";

        assertEquals(expected, metadata.toString());
    }
}
    
