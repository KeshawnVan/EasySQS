import org.apache.commons.lang3.StringUtils;

public class SimpleMessageUtil {


    public static boolean isFIFO(String queue) {
        //.fifo 结尾的为 FIFO队列
        return queue.endsWith(".fifo");
    }

    public static String getQueuePrefix() {
        return SqsConfig.getInstance().getQueuePrefix();
    }

    public static String generateFailQueue(String queue) {
        return StringUtils.replaceAll(MessageQueueService.FAIL_QUEUE_PREFIX + queue, getQueuePrefix(), "");
    }

    public static Boolean isFailQueue(String queue) {
        return queue.contains(MessageQueueService.FAIL_QUEUE_PREFIX);
    }
}
