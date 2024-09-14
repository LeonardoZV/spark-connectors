package com.leonardozv.spark.connectors.aws.dynamodb.write;

import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.model.BatchStatementErrorCodeEnum;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DynamoDbSinkWriteBuilderUnitTest {

    private LogicalWriteInfo info;
    private StructType schema;

    @BeforeEach
    void setUp() {
        schema = mock(StructType.class);
        when(schema.fieldIndex("statement")).thenReturn(0);

        info = mock(LogicalWriteInfo.class);
        when(info.schema()).thenReturn(schema);
    }

    @Test
    void testBuildWithDefaultOptions() {

        Map<String, String> optionsMap = new HashMap<>();

        when(info.options()).thenReturn(new CaseInsensitiveStringMap(optionsMap));

        DynamoDbSinkWriteBuilder builder = new DynamoDbSinkWriteBuilder(info);
        Write write = builder.build();

        assertNotNull(write);
        assertInstanceOf(DynamoDbSinkWrite.class, write);

        DynamoDbSinkOptions options = ((DynamoDbSinkWrite) write).options();

        assertEquals("us-east-1", options.region());
        assertNull(options.endpoint());
        assertEquals(25, options.batchSize());
        assertTrue(options.errorsToIgnore().isEmpty());
        assertEquals(0, options.statementColumnIndex());

    }

    @Test
    void testBuildWithCustomOptions() {
        Map<String, String> optionsMap = new HashMap<>();

        optionsMap.put("region", "us-west-2");
        optionsMap.put("endpoint", "http://localhost:8000");
        optionsMap.put("batchSize", "10");
        optionsMap.put("errorsToIgnore", "ConditionalCheckFailed,ProvisionedThroughputExceeded");

        when(info.options()).thenReturn(new CaseInsensitiveStringMap(optionsMap));

        DynamoDbSinkWriteBuilder builder = new DynamoDbSinkWriteBuilder(info);
        Write write = builder.build();

        assertNotNull(write);
        assertInstanceOf(DynamoDbSinkWrite.class, write);

        DynamoDbSinkOptions options = ((DynamoDbSinkWrite) write).options();
        assertEquals("us-west-2", options.region());
        assertEquals("http://localhost:8000", options.endpoint());
        assertEquals(10, options.batchSize());
        assertTrue(options.errorsToIgnore().contains(BatchStatementErrorCodeEnum.CONDITIONAL_CHECK_FAILED.toString()));
        assertTrue(options.errorsToIgnore().contains(BatchStatementErrorCodeEnum.PROVISIONED_THROUGHPUT_EXCEEDED.toString()));
        assertEquals(0, options.statementColumnIndex());
    }

}
