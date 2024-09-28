package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsSinkWriteBuilderUnitTest {

    @Test
    void testBuildWithDefaultOptionsAndWithoutMessageAttributesAndGroupId() {

        LogicalWriteInfo info = mock(LogicalWriteInfo.class);

        Map<String, String> optionsMap = new HashMap<String, String>(){{
            put("queueName", "test-queue");
        }};

        when(info.options()).thenReturn(new CaseInsensitiveStringMap(optionsMap));

        StructType schema = new StructType()
                .add("value", "string");

        when(info.schema()).thenReturn(schema);

        SqsSinkWriteBuilder builder = new SqsSinkWriteBuilder(info);

        SqsSinkWrite write = (SqsSinkWrite) builder.build();

        assertNotNull(write);
        assertEquals("", write.options().endpoint());
        assertEquals(Region.of("us-east-1"), write.options().region());
        assertEquals("test-queue", write.options().queueName());
        assertEquals("", write.options().queueOwnerAWSAccountId());
        assertEquals(10, write.options().batchSize());
        assertFalse(write.options().useSqsExtendedClient());
        assertEquals("", write.options().s3Endpoint());
        assertEquals(Region.of("us-east-1"), write.options().s3Region());
        assertFalse(write.options().forcePathStyle());
        assertEquals("", write.options().bucketName());
        assertEquals(262144, write.options().payloadSizeThreshold());
        assertEquals("", write.options().s3KeyPrefix());
        assertEquals(0, write.valueColumnIndex());
        assertEquals(-1, write.msgAttributesColumnIndex());
        assertEquals(-1, write.groupIdColumnIndex());

    }

    @Test
    void testBuildWithCustomOptionsAndWithMessageAttributesAndGroupId() {

        LogicalWriteInfo info = mock(LogicalWriteInfo.class);

        Map<String, String> optionsMap = new HashMap<String, String>(){{
            put("endpoint", "http://localhost:4566");
            put("region", "us-west-2");
            put("queueName", "test-queue");
            put("queueOwnerAWSAccountId", "123456789012");
            put("batchSize", "5");
            put("useSqsExtendedClient", "true");
            put("s3Endpoint", "http://localhost:4572");
            put("s3Region", "us-west-2");
            put("forcePathStyle", "true");
            put("bucketName", "test-bucket");
            put("payloadSizeThreshold", "1024");
            put("s3KeyPrefix", "Q0/");
        }};

        when(info.options()).thenReturn(new CaseInsensitiveStringMap(optionsMap));

        StructType schema = new StructType()
                .add("value", "string")
                .add("msg_attributes", "map<string,string>")
                .add("group_id", "string");

        when(info.schema()).thenReturn(schema);

        SqsSinkWriteBuilder builder = new SqsSinkWriteBuilder(info);

        SqsSinkWrite write = (SqsSinkWrite) builder.build();

        assertNotNull(write);
        assertInstanceOf(SqsSinkWrite.class, write);
        assertEquals("http://localhost:4566", write.options().endpoint());
        assertEquals(Region.of("us-west-2"), write.options().region());
        assertEquals("test-queue", write.options().queueName());
        assertEquals("123456789012", write.options().queueOwnerAWSAccountId());
        assertEquals(5, write.options().batchSize());
        assertTrue(write.options().useSqsExtendedClient());
        assertEquals("http://localhost:4572", write.options().s3Endpoint());
        assertEquals(Region.of("us-west-2"), write.options().s3Region());
        assertTrue(write.options().forcePathStyle());
        assertEquals("test-bucket", write.options().bucketName());
        assertEquals(1024, write.options().payloadSizeThreshold());
        assertEquals("Q0/", write.options().s3KeyPrefix());
        assertEquals(0, write.valueColumnIndex());
        assertEquals(1, write.msgAttributesColumnIndex());
        assertEquals(2, write.groupIdColumnIndex());

    }

}
