package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.*;

public class SqsSinkDataWriter implements DataWriter<InternalRow> {

    private final int partitionId;
    private final long taskId;
    private final SqsClient sqs;
    private final String queueUrl;
    private final SqsSinkOptions options;
    private final StructType schema;
    private final List<SendMessageBatchRequestEntry> messages = new ArrayList<>();

    public SqsSinkDataWriter(int partitionId, long taskId, SqsClient sqs, String queueUrl, SqsSinkOptions options, StructType schema) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.options = options;
        this.schema = schema;
    }

    @Override
    public void write(InternalRow row) {

        SendMessageBatchRequestEntry.Builder sendMessageBatchRequestEntryBuilder = SendMessageBatchRequestEntry.builder()
                .messageBody(row.getString(this.schema.fieldIndex("value")))
                .id(UUID.randomUUID().toString());

        if (!this.schema.getFieldIndex("delay_seconds").isEmpty()) {
            sendMessageBatchRequestEntryBuilder.delaySeconds(row.getInt(this.schema.fieldIndex("delay_seconds")));
        }

        if(!this.schema.getFieldIndex("message_attributes").isEmpty()) {
            sendMessageBatchRequestEntryBuilder.messageAttributes(convertMapDataToMapMessageAttributes(row.getMap(this.schema.fieldIndex("message_attributes"))));
        }

        if (!this.schema.getFieldIndex("message_deduplication_id").isEmpty()) {
            sendMessageBatchRequestEntryBuilder.messageDeduplicationId(row.getString(this.schema.fieldIndex("message_deduplication_id")));
        }

        if(!this.schema.getFieldIndex("message_group_id").isEmpty()) {
            sendMessageBatchRequestEntryBuilder.messageGroupId(row.getString(this.schema.fieldIndex("message_group_id")));
        }

        if(!this.schema.getFieldIndex("message_system_attributes").isEmpty()) {
            sendMessageBatchRequestEntryBuilder.messageSystemAttributesWithStrings(convertMapDataToMapMessageSystemAttributes(row.getMap(this.schema.fieldIndex("message_system_attributes"))));
        }

        SendMessageBatchRequestEntry sendMessageBatchRequestEntry = sendMessageBatchRequestEntryBuilder.build();

        this.messages.add(sendMessageBatchRequestEntry);

        if(this.messages.size() >= this.options.batchSize()) {
            sendMessages();
        }

    }

    @Override
    public WriterCommitMessage commit() {

        if(!this.messages.isEmpty()) {
            sendMessages();
        }

        return new SqsSinkWriterCommitMessage(this.partitionId, this.taskId);

    }

    @Override
    public void abort() {
        // nothing to abort here, since this sink is not atomic
    }

    @Override
    public void close() {
        // nothing to close
    }

    private Map<String, MessageAttributeValue> convertMapDataToMapMessageAttributes(MapData msgAttributesMapData) {

        Map<String, MessageAttributeValue> attributes = new HashMap<>();

        msgAttributesMapData.foreach(DataTypes.StringType, DataTypes.StringType, (key, value) -> {
            attributes.put(key.toString(), MessageAttributeValue.builder().dataType("String").stringValue(value.toString()).build());
            return null;
        });

        return attributes;

    }

    private Map<String, MessageSystemAttributeValue> convertMapDataToMapMessageSystemAttributes(MapData msgAttributesMapData) {

        Map<String, MessageSystemAttributeValue> attributes = new HashMap<>();

        msgAttributesMapData.foreach(DataTypes.StringType, DataTypes.StringType, (key, value) -> {
            attributes.put(key.toString(), MessageSystemAttributeValue.builder().dataType("String").stringValue(value.toString()).build());
            return null;
        });

        return attributes;

    }

    private void sendMessages() {

        SendMessageBatchRequest request = SendMessageBatchRequest.builder().queueUrl(this.queueUrl).entries(this.messages).build();

        SendMessageBatchResponse response = this.sqs.sendMessageBatch(request);

        List<BatchResultErrorEntry> errors = response.failed();

        if(!errors.isEmpty()) {
            throw new SqsSinkBatchResultException.Builder().withErrors(response.failed()).build();
        }

        this.messages.clear();

    }

}
