package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.BatchStatementErrorCodeEnum;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.*;

class DynamoDbSinkWriteBuilderUnitTest {

    @Test
    void testBuildWithDefaultOptions() {

        DynamoDbSinkOptions options = new DynamoDbSinkOptions(new HashMap<>());

        StructType schema = new StructType()
                .add("statement", "string");

        DynamoDbSinkWriteBuilder builder = new DynamoDbSinkWriteBuilder(options, schema);

        DynamoDbSinkWrite write = (DynamoDbSinkWrite) builder.build();

        assertNotNull(write);
        assertInstanceOf(DynamoDbSinkWrite.class, write);
        assertTrue(write.options().endpoint().isEmpty());
        assertEquals(Region.of("us-east-1"), write.options().region());
        assertEquals(25, write.options().batchSize());
        assertTrue(write.options().retryErrors().isEmpty());
        assertTrue(write.options().ignoreErrors().isEmpty());
        assertEquals(0, write.schema().getFieldIndex("statement").get());

    }

    @Test
    void testBuildWithCustomOptions() {

        DynamoDbSinkOptions options = new DynamoDbSinkOptions(new HashMap<>() {{
            put("endpoint", "http://localhost:8000");
            put("region", "us-west-2");
            put("batchSize", "3");
            put("ignoreErrors", "ConditionalCheckFailed,ProvisionedThroughputExceeded");
        }});

        StructType schema = new StructType()
                .add("statement", "string");

        DynamoDbSinkWriteBuilder builder = new DynamoDbSinkWriteBuilder(options, schema);

        DynamoDbSinkWrite write = (DynamoDbSinkWrite) builder.build();

        assertNotNull(write);
        assertInstanceOf(DynamoDbSinkWrite.class, write);
        assertEquals("http://localhost:8000", write.options().endpoint());
        assertEquals(Region.of("us-west-2"), write.options().region());
        assertEquals(3, write.options().batchSize());
        assertTrue(write.options().ignoreErrors().contains(BatchStatementErrorCodeEnum.CONDITIONAL_CHECK_FAILED.toString()));
        assertTrue(write.options().ignoreErrors().contains(BatchStatementErrorCodeEnum.PROVISIONED_THROUGHPUT_EXCEEDED.toString()));
        assertEquals(0, write.schema().getFieldIndex("statement").get());

    }

}
