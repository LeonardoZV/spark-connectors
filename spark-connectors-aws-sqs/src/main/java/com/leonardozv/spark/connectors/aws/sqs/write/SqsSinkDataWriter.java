package com.leonardozv.spark.connectors.aws.sqs.write;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.BatchResultErrorEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqsSinkDataWriter implements DataWriter<InternalRow> {

    private final int partitionId;
    private final long taskId;
    private final SqsClient sqs;
    private final String queueUrl;
    private final SqsSinkOptions options;
    private final StructType schema;
    private final Retry retry;
    private HashMap<String, SendMessageBatchRequestEntry> messages = new LinkedHashMap<>();

    public SqsSinkDataWriter(int partitionId, long taskId, SqsClient sqs, String queueUrl, SqsSinkOptions options, StructType schema) {

        this.partitionId = partitionId;
        this.taskId = taskId;
        this.sqs = sqs;
        this.queueUrl = queueUrl;
        this.options = options;
        this.schema = schema;

        IntervalFunction intervalFunction = IntervalFunction
                .ofExponentialRandomBackoff(this.options.retryInitialInterval(), this.options.retryMultiplier(), this.options.retryRandomizationFactor(), this.options.retryMaxInterval());

        Set<String> retryExceptionsWithRetryableErrors = Stream
                .concat(this.options.retryExceptions().stream(), Stream.of(ResponseContainsRetryableErrorsException.class.getName()))
                .collect(Collectors.toSet());

        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(this.options.retryMaxAttempts())
                .intervalFunction(intervalFunction)
                .retryExceptions(SqsSinkParsers.parseExceptions(retryExceptionsWithRetryableErrors))
                .ignoreExceptions(SqsSinkParsers.parseExceptions(this.options.ignoreExceptions()))
                .build();

        this.retry = Retry.of("sendMessages", retryConfig);

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
            sendMessageBatchRequestEntryBuilder.messageAttributes(SqsSinkParsers.parseMapMessageAttributes(row.getMap(this.schema.fieldIndex("message_attributes"))));
        }

        if (!this.schema.getFieldIndex("message_deduplication_id").isEmpty()) {
            sendMessageBatchRequestEntryBuilder.messageDeduplicationId(row.getString(this.schema.fieldIndex("message_deduplication_id")));
        }

        if(!this.schema.getFieldIndex("message_group_id").isEmpty()) {
            sendMessageBatchRequestEntryBuilder.messageGroupId(row.getString(this.schema.fieldIndex("message_group_id")));
        }

        if(!this.schema.getFieldIndex("message_system_attributes").isEmpty()) {
            sendMessageBatchRequestEntryBuilder.messageSystemAttributesWithStrings(SqsSinkParsers.parseMapMessageSystemAttributes(row.getMap(this.schema.fieldIndex("message_system_attributes"))));
        }

        SendMessageBatchRequestEntry sendMessageBatchRequestEntry = sendMessageBatchRequestEntryBuilder.build();

        this.messages.put(sendMessageBatchRequestEntry.id(), sendMessageBatchRequestEntry);

        if(this.messages.size() >= this.options.batchSize()) {
            Retry.decorateRunnable(retry, this::sendMessages).run();
        }

    }

    @Override
    public WriterCommitMessage commit() {

        if(!this.messages.isEmpty()) {
            Retry.decorateRunnable(retry, this::sendMessages).run();
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

    private void sendMessages() {

        SendMessageBatchRequest request = SendMessageBatchRequest.builder()
                .queueUrl(this.queueUrl)
                .entries(this.messages.values())
                .build();

        SendMessageBatchResponse response = this.sqs.sendMessageBatch(request);

        HashMap<String, SendMessageBatchRequestEntry> retryableMessages = new LinkedHashMap<>();

        List<BatchResultErrorEntry> nonRetryableAndNonIgnorableErrors = new ArrayList<>();

        response.failed().forEach(failedResponse -> {

            if (this.options.retryErrors().contains(failedResponse.code())) {
                SendMessageBatchRequestEntry failedMessage = this.messages.get(failedResponse.id());
                retryableMessages.put(failedMessage.id(), failedMessage);
            } else {
                if (!this.options.ignoreErrors().contains(failedResponse.code())) {
                    nonRetryableAndNonIgnorableErrors.add(failedResponse);
                }
            }

        });

        if (!nonRetryableAndNonIgnorableErrors.isEmpty()) {
            throw new ResponseContainsNonRetryableErrorsException.Builder()
                    .withErrors(nonRetryableAndNonIgnorableErrors)
                    .build();
        }

        this.messages = retryableMessages;

        if (!this.messages.isEmpty()) {
            throw new ResponseContainsRetryableErrorsException.Builder()
                    .withErrors(nonRetryableAndNonIgnorableErrors)
                    .build();
        }

    }

}
