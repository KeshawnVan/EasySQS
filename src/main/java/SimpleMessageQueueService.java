import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimpleMessageQueueService implements MessageQueueService {

    private static final int BATCH = 10;

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleMessageQueueService.class);

    private AmazonSQS sqs;

    private Map<String, String> urlCaches = new ConcurrentHashMap<>();

    public SimpleMessageQueueService() {

        sqs = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(SqsConfig.getInstance().getAccessKeyId(), SqsConfig.getInstance().getSecretKey())))
                .withRegion(SqsConfig.getInstance().getRegion())
                .build();

        getAllQueueByPrefix(SqsConfig.getInstance().getQueuePrefix()).forEach(queue -> urlCaches.put(queue, getQueueUrl(queue)));
    }

    @Override
    public void sendMessage(String queue, Message message) {
        LOGGER.debug("start publish to queue:{} , messageId:{}", queue, message.getMessageId());

        checkMessageId(message);

        String url = getQueueUrl(queue);
        SendMessageRequest sendMessageRequest = new SendMessageRequest(url, message.getContent());

        if (isFIFO(queue)) {
            if (StringUtils.isEmpty(message.getMqMessageGroupId())) {
                message.setMqMessageGroupId(queue + "-default-group-id");
            }
            sendMessageRequest.setMessageGroupId(message.getMqMessageGroupId()); //only to FIFO
            sendMessageRequest.setMessageDeduplicationId(message.getMessageId()); //only to FIFO
        }

        SendMessageResult sendMessageResult = sqs.sendMessage(sendMessageRequest);
        String mqMessageId = sendMessageResult.getMessageId();
        message.setMessageId(mqMessageId);
    }

    @Override
    public void sendMessages(String queue, List<Message> messages) {
        messages = CollectionUtils.isNotEmpty(messages) ? messages : Collections.emptyList();

        String url = getQueueUrl(queue);
        boolean isFIFO = isFIFO(queue);

        List<SendMessageBatchRequestEntry> sendMessageBatchRequestEntries = messages.stream().map(message -> {
            checkMessageId(message);
            SendMessageBatchRequestEntry sendMessageBatchRequestEntry = new SendMessageBatchRequestEntry(message.getMessageId(), message.getContent());
            if (isFIFO) {
                if (StringUtils.isEmpty(message.getMqMessageGroupId())) {
                    LOGGER.warn("SQS send message to fifo queue {} Message Group Id cannot be null", queue);
                    message.setMqMessageGroupId(queue + "-default-group-id");
                }
                sendMessageBatchRequestEntry.setMessageGroupId(message.getMqMessageGroupId()); //only to FIFO
                sendMessageBatchRequestEntry.setMessageDeduplicationId(message.getMessageId()); //only to FIFO
            }
            return sendMessageBatchRequestEntry;
        }).collect(Collectors.toList());

        Lists.partition(sendMessageBatchRequestEntries, BATCH).stream().forEach(requestEntry -> sqs.sendMessageBatch(url, requestEntry));
    }

    @Override
    public Set<String> getAllQueueByPrefix(String prefix) {
        return sqs.listQueues(formatQueueName(prefix)).getQueueUrls().stream()
                .map(x -> x.substring(x.lastIndexOf("/") + 1))
                .collect(Collectors.toSet());
    }

    @Override
    public String listen(Listener listener) {
        String queue = listener.getQueue();
        LOGGER.info("create listener for queue : {}", queue);
        Runnable runnable = () -> {
            Thread.currentThread().setName(queue + "-" + LocalDateTime.now());
            Stream.generate(() -> receiveMessages(queue, listener.getNumber(), 20, listener.getVisibilityTimeout()))
                    .filter(CollectionUtils::isNotEmpty).forEach(messages -> {
                try {
                    List<Message> ackMessages = listener.getMessagesFunction().apply(messages);
                    ack(ackMessages);
                } catch (Throwable e) {//NOSONAR
                    LOGGER.error("Listen queue : " + queue + " handleDelivery error message is : " + messages + " exception detail :  ", e);
                    ack(messages);
                    sendMessages(SimpleMessageUtil.generateFailQueue(queue), messages);
                }
            });
        };
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(runnable);
        return listener.getId();
    }

    private String formatQueueName(String queue) {
        return queue.startsWith(SimpleMessageUtil.getQueuePrefix()) ? queue : SimpleMessageUtil.getQueuePrefix() + queue;
    }

    private static boolean isFIFO(String queue) {
        //.fifo 结尾的为 FIFO队列
        return queue.endsWith(".fifo");
    }

    private String getQueueUrl(String queue) {
        queue = formatQueueName(queue);
        String url = urlCaches.get(queue);
        if (url == null) {
            try {
                url = sqs.getQueueUrl(queue).getQueueUrl();
            } catch (AmazonServiceException e) {
                if ("AWS.SimpleQueueService.NonExistentQueue".equals(e.getErrorCode())) {
                    url = createQueue(queue);
                } else {
                    throw e;
                }
            }
            urlCaches.put(queue, url);
        }
        return url;
    }

    public String createQueue(String queue) {
        return createDelayQueue(queue, null);
    }

    public String createDelayQueue(String queue, Integer delaySeconds) {
        return SimpleMessageUtil.isFailQueue(queue)
                ? createDelayQueue(queue, null, null)
                : createQueueWithDeadLetter(queue, delaySeconds);
    }

    private String createDelayQueue(String queue, Integer delaySeconds, String deadLetterTargetArn) {
        queue = formatQueueName(queue);

        if (urlCaches.containsKey(queue)) {
            return urlCaches.get(queue);
        }

        CreateQueueRequest r = new CreateQueueRequest(queue);
        if (isFIFO(queue)) {
            r.addAttributesEntry(QueueAttributeName.FifoQueue.toString(), "true");
            r.addAttributesEntry(QueueAttributeName.ContentBasedDeduplication.toString(), "true");
            r.addAttributesEntry(QueueAttributeName.VisibilityTimeout.toString(), "600");
            Optional.ofNullable(delaySeconds).ifPresent(delay -> r.addAttributesEntry(QueueAttributeName.DelaySeconds.toString(), delay.toString()));
            Optional.ofNullable(deadLetterTargetArn).ifPresent(arn -> r.addAttributesEntry(QueueAttributeName.RedrivePolicy.toString(), buildRedrivePolicy(arn)));
        }
        return sqs.createQueue(r).getQueueUrl();
    }

    private String buildRedrivePolicy(String arn) {
        return "{\"maxReceiveCount\":\"3\", \"deadLetterTargetArn\":\"" + arn + "\"}";
    }

    private String createQueueWithDeadLetter(String queue, Integer delaySeconds) {
        String deadLetterQueueUrl = getQueueUrl(SimpleMessageUtil.generateFailQueue(queue));
        String deadLetterQueueArn = getQueueArn(deadLetterQueueUrl);
        return createDelayQueue(queue, delaySeconds, deadLetterQueueArn);
    }

    private String getQueueArn(String queueUrl) {
        return sqs.getQueueAttributes(new GetQueueAttributesRequest(queueUrl).withAttributeNames("QueueArn"))
                .getAttributes().get("QueueArn");
    }

    private void checkMessageId(Message message) {
        if (StringUtils.isEmpty(message.getMessageId())) {
            message.setMessageId(UUID.randomUUID().toString());
        }
    }

    private List<Message> receiveMessages(String queue, int maxNumberOfMessages, int waitTimeSeconds, int visibilityTimeout) {
        try {
            final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(getQueueUrl(queue));

            List<com.amazonaws.services.sqs.model.Message> sqsMessages = maxNumberOfMessages <= BATCH
                    ? blockReceive(visibilityTimeout, receiveMessageRequest, waitTimeSeconds)
                    : loopReceive(visibilityTimeout, maxNumberOfMessages, receiveMessageRequest);

            return sqsMessages.stream().map(sqsMessage -> convert(sqsMessage, queue)).collect(Collectors.toList());
        } catch (Exception e) {
            LOGGER.error("sqs receiveMessages error", e);
            return Collections.emptyList();
        }
    }

    private List<com.amazonaws.services.sqs.model.Message> blockReceive(int visibilityTimeout, ReceiveMessageRequest receiveMessageRequest, int waitTimeSeconds) {
        receiveMessageRequest.setMaxNumberOfMessages(BATCH);
        receiveMessageRequest.setWaitTimeSeconds(waitTimeSeconds);
        receiveMessageRequest.setVisibilityTimeout(visibilityTimeout);
        return sqs.receiveMessage(receiveMessageRequest).getMessages();
    }

    private List<com.amazonaws.services.sqs.model.Message> loopReceive(int visibilityTimeout, int maxNumberOfMessages, ReceiveMessageRequest receiveMessageRequest) {
        List<com.amazonaws.services.sqs.model.Message> sqsMessages = new ArrayList<>(maxNumberOfMessages);
        for (int i = maxNumberOfMessages; i > 0; i -= BATCH) {
            int num = i > BATCH ? BATCH : i;
            receiveMessageRequest.setMaxNumberOfMessages(num);
            receiveMessageRequest.setVisibilityTimeout(visibilityTimeout);
            List<com.amazonaws.services.sqs.model.Message> receiveMessages = sqs.receiveMessage(receiveMessageRequest).getMessages();
            sqsMessages.addAll(receiveMessages);
            if (receiveMessages.size() < 5) {
                break;
            }
        }
        return sqsMessages;
    }

    private Message convert(com.amazonaws.services.sqs.model.Message message, String queue) {
        Message bossMessage = new Message(message.getMessageId(), message.getBody());
        bossMessage.setMqReceiptHandle(message.getReceiptHandle());
        bossMessage.setQueue(queue);
        return bossMessage;
    }

    public void ack(Message advancedMessage) {
        Optional.ofNullable(advancedMessage)
                .ifPresent(message -> sqs.deleteMessage(new DeleteMessageRequest(getQueueUrl(message.getQueue()), message.getMqReceiptHandle())));
    }

    public void ack(List<Message> advancedMessages) {
        if (CollectionUtils.isNotEmpty(advancedMessages)) {
            String url = getQueueUrl(advancedMessages.stream().findAny().map(Message::getQueue).orElse(null));

            List<DeleteMessageBatchRequestEntry> deleteRequests = advancedMessages.parallelStream()
                    .map(advancedMessage -> new DeleteMessageBatchRequestEntry(advancedMessage.getMessageId(), advancedMessage.getMqReceiptHandle()))
                    .collect(Collectors.toList());

            Lists.partition(deleteRequests, BATCH).forEach(request -> sqs.deleteMessageBatch(new DeleteMessageBatchRequest(url, request)));
        }
    }
}
