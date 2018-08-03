import org.junit.Test;

public class SqsTest {

    private static final String QUEUE = "recharge-result";

    private MessageQueueService messageQueueService = new SimpleMessageQueueService();

    @Test
    public void sendMessage() {
        messageQueueService.sendMessage(QUEUE, new Message("ok"));
    }

    public static void main(String[] args) {
        Listener listener = Listener.newBatchListener(QUEUE, messages -> {
            System.out.println(messages);
            return messages;
        }).build();
        new SimpleMessageQueueService().listen(listener);
    }
}
