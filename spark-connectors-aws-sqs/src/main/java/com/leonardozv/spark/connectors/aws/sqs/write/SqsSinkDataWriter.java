package com.leonardozv.spark.connectors.aws.sqs.write;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.DataTypes;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.*;

public class SqsSinkDataWriter implements DataWriter<InternalRow> {

    private final int partitionId;
    private final long taskId;
    private final SqsClient sqs;
    private final String queueUrl;
    private final SqsSinkOptions options;
    private final int valueColumnIndex;
    private final int msgAttributesColumnIndex;
    private final int groupIdColumnIndex;
    private final List<SendMessageBatchRequestEntry> messages = new ArrayList<>();

    public SqsSinkDataWriter(int partitionId, long taskId, SqsClient sqs, String queueUrl, SqsSinkOptions options, int valueColumnIndex, int msgAttributesColumnIndex, int groupIdColumnIndex) {
        this.partitionId = partitionId;
        this.taskId = taskId;
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.options = options;
        this.valueColumnIndex = valueColumnIndex;
        this.msgAttributesColumnIndex = msgAttributesColumnIndex;
        this.groupIdColumnIndex = groupIdColumnIndex;
    }

    @Override
    public void write(InternalRow row) {

        Optional<MapData> msgAttributesData = Optional.empty();

        if(this.msgAttributesColumnIndex >= 0) {
            msgAttributesData = Optional.of(row.getMap(this.msgAttributesColumnIndex));
        }

        SendMessageBatchRequestEntry.Builder sendMessageBatchRequestEntryBuilder = SendMessageBatchRequestEntry.builder()
                .messageBody(row.getString(this.valueColumnIndex))
                .messageAttributes(convertMapData(msgAttributesData))
                .id(UUID.randomUUID().toString());

        if(this.groupIdColumnIndex >= 0) {
            sendMessageBatchRequestEntryBuilder.messageGroupId(row.getString(this.groupIdColumnIndex));
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

    private Map<String, MessageAttributeValue> convertMapData(Optional<MapData> arrayData) {

        Map<String, MessageAttributeValue> attributes = new HashMap<>();

        arrayData.ifPresent(mapData -> mapData.foreach(DataTypes.StringType, DataTypes.StringType, (key, value) -> {
            attributes.put(key.toString(), MessageAttributeValue.builder().dataType("String").stringValue(value.toString()).build());
            return null;
        }));

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
