package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.BatchStatementErrorCodeEnum;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DynamoDbSinkWriteBuilderUnitTest {

    @Test
    void testBuildWithDefaultOptions() {

        LogicalWriteInfo info = mock(LogicalWriteInfo.class);

        Map<String, String> optionsMap = new HashMap<>();

        when(info.options()).thenReturn(new CaseInsensitiveStringMap(optionsMap));

        StructType schema = new StructType()
                .add("statement", "string");

        when(info.schema()).thenReturn(schema);

        DynamoDbSinkWriteBuilder builder = new DynamoDbSinkWriteBuilder(info);

        DynamoDbSinkWrite write = (DynamoDbSinkWrite) builder.build();

        assertNotNull(write);
        assertInstanceOf(DynamoDbSinkWrite.class, write);
        assertTrue(write.options().endpoint().isEmpty());
        assertEquals(Region.of("us-east-1"), write.options().region());
        assertEquals(25, write.options().batchSize());
        assertTrue(write.options().errorsToIgnore().isEmpty());
        assertEquals(0, write.statementColumnIndex());

    }

    @Test
    void testBuildWithCustomOptions() {

        LogicalWriteInfo info = mock(LogicalWriteInfo.class);

        Map<String, String> optionsMap = new HashMap<String, String>(){{
            put("endpoint", "http://localhost:8000");
            put("region", "us-west-2");
            put("batchSize", "3");
            put("errorsToIgnore", "ConditionalCheckFailed,ProvisionedThroughputExceeded");
        }};

        when(info.options()).thenReturn(new CaseInsensitiveStringMap(optionsMap));

        StructType schema = new StructType()
                .add("statement", "string");

        when(info.schema()).thenReturn(schema);

        DynamoDbSinkWriteBuilder builder = new DynamoDbSinkWriteBuilder(info);

        DynamoDbSinkWrite write = (DynamoDbSinkWrite) builder.build();

        assertNotNull(write);
        assertInstanceOf(DynamoDbSinkWrite.class, write);
        assertEquals("http://localhost:8000", write.options().endpoint());
        assertEquals(Region.of("us-west-2"), write.options().region());
        assertEquals(3, write.options().batchSize());
        assertTrue(write.options().errorsToIgnore().contains(BatchStatementErrorCodeEnum.CONDITIONAL_CHECK_FAILED.toString()));
        assertTrue(write.options().errorsToIgnore().contains(BatchStatementErrorCodeEnum.PROVISIONED_THROUGHPUT_EXCEEDED.toString()));
        assertEquals(0, write.statementColumnIndex());

    }

}
