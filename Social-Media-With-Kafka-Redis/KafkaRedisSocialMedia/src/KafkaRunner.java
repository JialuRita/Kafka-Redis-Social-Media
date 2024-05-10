public class KafkaRunner {
    public static void main(String[] args) {
        startSocialMediaApp();
    }

    private static void startSocialMediaApp() {
        Thread consumerThread = createConsumerThread();
        Thread producerThread = createProducerThread();

        consumerThread.start();
        producerThread.start();

        try {
            producerThread.join(); // 等待生产者线程结束
            consumerThread.join(); // 等待消费者线程结束
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted.");
            Thread.currentThread().interrupt(); // 重新设置中断标志
        }
    }

    private static Thread createConsumerThread() {
        return new Thread(() -> {
            try {
                SocialMediaConsumer.main(new String[]{});
            } catch (Exception e) {
                System.err.println("Consumer thread encountered an error: " + e.getMessage());
                e.printStackTrace();
            }
        }, "Consumer-Thread");
    }

    private static Thread createProducerThread() {
        return new Thread(() -> {
            try {
                Thread.sleep(500); // 延时启动生产者以等待消费者就绪
                SocialMediaProducer.main(new String[]{});
            } catch (InterruptedException e) {
                System.err.println("Producer thread interrupted.");
                Thread.currentThread().interrupt(); // 重新设置中断标志
            } catch (Exception e) {
                System.err.println("Producer thread encountered an error: " + e.getMessage());
                e.printStackTrace();
            }
        }, "Producer-Thread");
    }
}
