import java.io.Serializable;

public final class Message implements Serializable {

    /**
     * 消息标识
     */
    private String messageId;

    /**
     * 消息内容
     */
    private String content;

    /**
     * 消息分组，用于SQS FIFO消息
     */
    private String mqMessageGroupId;

    /**
     * 用于SQS确认消息使用
     */
    private String queue;

    /**
     * 用于SQS，消息中间件生成的回调句柄
     */
    private String mqReceiptHandle;

    public Message() {

    }

    public Message(String content) {
        this.content = content;
    }

    public Message(String messageId, String content) {
        this.messageId = messageId;
        this.content = content;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getMqMessageGroupId() {
        return mqMessageGroupId;
    }

    public void setMqMessageGroupId(String mqMessageGroupId) {
        this.mqMessageGroupId = mqMessageGroupId;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getMqReceiptHandle() {
        return mqReceiptHandle;
    }

    public void setMqReceiptHandle(String mqReceiptHandle) {
        this.mqReceiptHandle = mqReceiptHandle;
    }

    @Override
    public String toString() {
        return "Message{" + "id =" + messageId + ", content =" + content + '}';
    }
}
