import java.util.List;
import java.util.UUID;
import java.util.function.Function;

public class Listener {

    private String id;

    private String queue;

    private int visibilityTimeout;

    private int number;

    private int idleInterval;

    private Function<Message, Message> messageFunction;

    private Function<List<Message>, List<Message>> messagesFunction;

    public Listener(String queue, int visibilityTimeout, int number, int idleInterval, Function<Message, Message> messageFunction, Function<List<Message>, List<Message>> messagesFunction) {
        this.id = UUID.randomUUID().toString();
        this.queue = queue;
        this.visibilityTimeout = visibilityTimeout;
        this.number = number;
        this.idleInterval = idleInterval;
        this.messageFunction = messageFunction;
        this.messagesFunction = messagesFunction;
    }

    public String getQueue() {
        return queue;
    }

    public int getVisibilityTimeout() {
        return visibilityTimeout;
    }

    public int getNumber() {
        return number;
    }

    public int getIdleInterval() {
        return idleInterval;
    }

    public Function<Message, Message> getMessageFunction() {
        return messageFunction;
    }

    public Function<List<Message>, List<Message>> getMessagesFunction() {
        return messagesFunction;
    }

    public String getId() {
        return id;
    }

    public static ListenerBuilder newListener(String queue, Function<Message, Message> messageFunction) {
        return new ListenerBuilder(queue, messageFunction);
    }

    public static BatchListenerBuilder newBatchListener(String queue, Function<List<Message>, List<Message>> messagesFunction) {
        return new BatchListenerBuilder(queue, messagesFunction);
    }

    public static class ListenerBuilder {

        private String queue;

        private int visibilityTimeout = 100;

        private int idleInterval = 5;

        private Function<Message, Message> messageFunction;

        public ListenerBuilder(String queue, Function<Message, Message> messageFunction) {
            this.queue = queue;
            this.messageFunction = messageFunction;
        }

        public ListenerBuilder visibilityTimeout(int visibilityTimeout) {
            this.visibilityTimeout = visibilityTimeout;
            return this;
        }

        public ListenerBuilder idleInterval(int idleInterval) {
            this.idleInterval = idleInterval;
            return this;
        }

        public Listener build() {
            return new Listener(queue, visibilityTimeout, 1, idleInterval, messageFunction, null);
        }
    }

    public static class BatchListenerBuilder {

        private String queue;

        private int number = 10;

        private int visibilityTimeout = 100;

        private int idleInterval = 5;

        private Function<List<Message>, List<Message>> messagesFunction;

        public BatchListenerBuilder(String queue, Function<List<Message>, List<Message>> messagesFunction) {
            this.queue = queue;
            this.messagesFunction = messagesFunction;
        }

        public BatchListenerBuilder number(int number) {
            this.number = number;
            return this;
        }

        public BatchListenerBuilder visibilityTimeout(int visibilityTimeout) {
            this.visibilityTimeout = visibilityTimeout;
            return this;
        }

        public BatchListenerBuilder idleInterval(int idleInterval) {
            this.idleInterval = idleInterval;
            return this;
        }

        public Listener build() {
            return new Listener(queue, visibilityTimeout, number, idleInterval, null, messagesFunction);
        }
    }
}
