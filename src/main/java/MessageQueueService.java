import java.util.List;
import java.util.Set;
import java.util.function.Function;

public interface MessageQueueService {

    String FAIL_QUEUE_PREFIX = "FAIL-";

    /**
     * 发送消息到指定queue
     * 如果队列不存在，自动创建
     * 消息队列不可用时会重试重新投递
     *
     * @param queue
     * @param message
     * @return
     */
    void sendMessage(String queue, Message message);

    /**
     * 批量发送消息到指定queue
     *
     * @param queue
     * @param messages
     * @return
     */
    void sendMessages(String queue, List<Message> messages);

    /**
     * 根据前缀获取queue
     *
     * @param prefix
     * @return
     */
    Set<String> getAllQueueByPrefix(String prefix);

    String listen(Listener listener);
}
