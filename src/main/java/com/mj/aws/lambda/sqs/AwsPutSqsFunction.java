package com.mj.aws.lambda.sqs;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.*;
import com.mj.aws.lambda.sqs.config.AppConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Slf4j
@Component("awsLambdaPutSqsFunction")
public class AwsPutSqsFunction implements Function<Void,Void>{

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private AmazonSQS amazonSqs;



    @Override
    public Void apply(Void empty) {
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put("AttributeOne", new MessageAttributeValue()
                .withStringValue("This is an attribute")
                .withDataType("String"));

        SendMessageRequest sendMessageStandardQueue = new SendMessageRequest()
                .withQueueUrl(appConfig.getQueueUrl())
                .withMessageBody("A simple message.")
                .withDelaySeconds(0)
                .withMessageAttributes(messageAttributes);

        amazonSqs.sendMessage(sendMessageStandardQueue);
        return empty;
    }
    private void deleteMessagesByQueueUrl(List<Message> messageList, String queueUrl) {

        if(messageList == null || messageList.isEmpty()){
            log.info("Empty list for removal");
            return;
        }

        List<DeleteMessageBatchRequestEntry> deleteMessageEntries = new ArrayList<>();

        messageList.forEach(message ->
            deleteMessageEntries.add(
                    new DeleteMessageBatchRequestEntry(message.getMessageId(),message.getReceiptHandle())
        ));

        DeleteMessageBatchRequest deleteRequest = new DeleteMessageBatchRequest();
        deleteRequest.setQueueUrl(queueUrl);
        deleteRequest.setEntries(deleteMessageEntries);

        DeleteMessageBatchResult result = amazonSqs.deleteMessageBatch(deleteRequest);

        log.info("Successfully removed size: {}",result.getSuccessful().size());
        log.error("Failed to removed size: {}",result.getFailed().size());
    }

    private List<Message> getQueueMessageByQueueUrl(String queueUrl) {
        log.info("Getting messages from queue url: {}", queueUrl);

        ReceiveMessageRequest messageRequest = new ReceiveMessageRequest(queueUrl).
                withWaitTimeSeconds(5).withMaxNumberOfMessages(2);

        List<Message> messages = amazonSqs.receiveMessage(messageRequest).getMessages();

        log.info("Received total messages size: {}", messages.size());

        return messages;
    }


}
