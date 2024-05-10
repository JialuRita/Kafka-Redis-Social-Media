import redis.clients.jedis.Jedis;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class SocialMediaRedis {
    private static final String REDIS_HOST = "localhost";  //Redis host
    private static final int REDIS_PORT = 6379;  //Redis port

    private static Properties createConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "social-media-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static void consumeMessages(KafkaConsumer<String, String> consumer, Jedis jedis) {
        final int giveUp = 10000;
        int noRecordsCount = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records.count() == 0) {
                noRecordsCount += 100;
                if (noRecordsCount > giveUp) break;
            } else {
                noRecordsCount = 0;
                System.out.println("Received " + records.count() + " records");
                records.forEach(record -> processRecord(record, jedis));
            }
        }
    }

    private static void processRecord(ConsumerRecord<String, String> record, Jedis jedis) {
        String[] parts = record.value().split(" ");
        String topic = record.topic();
        String userWhoPosted = parts[1];
        String postId = parts[2];
        //three topics
        switch (topic) {
            case "likes":
                jedis.hincrBy("likes:" + userWhoPosted, postId, 1);
                break;
            case "comments":
                String comment = parts[3];
                jedis.rpush("comments:" + userWhoPosted, comment);
                break;
            case "shares":
                int shareCount = parts.length - 3;
                jedis.incrBy("popularity:" + userWhoPosted, 20 * shareCount);
                break;
        }
    }

    public static void main(String[] args) {
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createConsumerProperties())) {
            System.out.println("Connected to Redis");
            consumer.subscribe(Arrays.asList("likes", "comments", "shares"));
            consumeMessages(consumer, jedis);
        } catch (Exception e) {
            System.err.println("Error in processing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("Consumer and Redis client closed");
        }
    }
}
