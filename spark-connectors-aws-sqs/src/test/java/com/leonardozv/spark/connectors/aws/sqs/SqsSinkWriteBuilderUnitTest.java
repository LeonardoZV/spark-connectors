package com.leonardozv.spark.connectors.aws.sqs;

import com.leonardozv.spark.connectors.aws.sqs.write.SqsSinkOptions;
import com.leonardozv.spark.connectors.aws.sqs.write.SqsSinkWrite;
import com.leonardozv.spark.connectors.aws.sqs.write.SqsSinkWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SqsSinkWriteBuilderUnitTest {

    @Test
    void testBuild() {

        LogicalWriteInfo info = mock(LogicalWriteInfo.class);

        Map<String, String> options = new HashMap<>();

        options.put("region", "us-west-2");
        options.put("sqsEndpoint", "http://localhost:4566");
        options.put("queueName", "test-queue");
        options.put("queueOwnerAWSAccountId", "123456789012");
        options.put("batchSize", "5");
        options.put("useSqsExtendedClient", "true");
        options.put("s3Endpoint", "http://localhost:4572");
        options.put("forcePathStyle", "true");
        options.put("bucketName", "test-bucket");
        options.put("payloadSizeThreshold", "1024");

        when(info.options()).thenReturn(new CaseInsensitiveStringMap(options));

        StructType schema = new StructType()
                .add("value", "string")
                .add("msg_attributes", "map<string,string>")
                .add("group_id", "string");

        when(info.schema()).thenReturn(schema);

        // Create SqsSinkWriteBuilder
        SqsSinkWriteBuilder builder = new SqsSinkWriteBuilder(info);

        // Build SqsSinkWrite
        SqsSinkWrite write = (SqsSinkWrite) builder.build();

        // Verify SqsSinkOptions
        SqsSinkOptions sinkOptions = write.options();
        assertEquals("us-west-2", sinkOptions.region());
        assertEquals("http://localhost:4566", sinkOptions.sqsEndpoint());
        assertEquals("test-queue", sinkOptions.queueName());
        assertEquals("123456789012", sinkOptions.queueOwnerAWSAccountId());
        assertEquals(5, sinkOptions.batchSize());
        assertTrue(sinkOptions.useSqsExtendedClient());
        assertEquals("http://localhost:4572", sinkOptions.s3Endpoint());
        assertTrue(sinkOptions.forcePathStyle());
        assertEquals("test-bucket", sinkOptions.bucketName());
        assertEquals(1024, sinkOptions.payloadSizeThreshold());
        assertEquals(0, sinkOptions.valueColumnIndex());
        assertEquals(1, sinkOptions.msgAttributesColumnIndex());
        assertEquals(2, sinkOptions.groupIdColumnIndex());

    }

    @Test
    void testBuildWithoutMessageAttributesAndGroupId() {

        LogicalWriteInfo info = mock(LogicalWriteInfo.class);

        Map<String, String> options = new HashMap<>();

        options.put("region", "us-west-2");
        options.put("sqsEndpoint", "http://localhost:4566");
        options.put("queueName", "test-queue");
        options.put("queueOwnerAWSAccountId", "123456789012");
        options.put("batchSize", "5");
        options.put("useSqsExtendedClient", "true");
        options.put("s3Endpoint", "http://localhost:4572");
        options.put("forcePathStyle", "true");
        options.put("bucketName", "test-bucket");
        options.put("payloadSizeThreshold", "1024");

        when(info.options()).thenReturn(new CaseInsensitiveStringMap(options));

        StructType schema = new StructType()
                .add("value", "string");

        when(info.schema()).thenReturn(schema);

        // Create SqsSinkWriteBuilder
        SqsSinkWriteBuilder builder = new SqsSinkWriteBuilder(info);

        // Build SqsSinkWrite
        SqsSinkWrite write = (SqsSinkWrite) builder.build();

        // Verify SqsSinkOptions
        SqsSinkOptions sinkOptions = write.options();
        assertEquals("us-west-2", sinkOptions.region());
        assertEquals("http://localhost:4566", sinkOptions.sqsEndpoint());
        assertEquals("test-queue", sinkOptions.queueName());
        assertEquals("123456789012", sinkOptions.queueOwnerAWSAccountId());
        assertEquals(5, sinkOptions.batchSize());
        assertTrue(sinkOptions.useSqsExtendedClient());
        assertEquals("http://localhost:4572", sinkOptions.s3Endpoint());
        assertTrue(sinkOptions.forcePathStyle());
        assertEquals("test-bucket", sinkOptions.bucketName());
        assertEquals(1024, sinkOptions.payloadSizeThreshold());
        assertEquals(0, sinkOptions.valueColumnIndex());
        assertEquals(-1, sinkOptions.msgAttributesColumnIndex());
        assertEquals(-1, sinkOptions.groupIdColumnIndex());

    }

}
