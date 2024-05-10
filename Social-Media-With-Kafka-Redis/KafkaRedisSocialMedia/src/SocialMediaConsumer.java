import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SocialMediaConsumer {
    private static final ObjectMapper mapper = new ObjectMapper();

    private static Properties loadConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "social-media-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private static void processMessages(KafkaConsumer<String, String> consumer) {
        Map<String, Set<String>> userComments = new HashMap<>();
        Map<String, Map<String, Integer>> userLikes = new HashMap<>();
        Map<String, Integer> userPopularity = new HashMap<>();

        final int giveUp = 10000; // 10 seconds timeout
        int noRecordsCount = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            if (records.count() == 0) {
                noRecordsCount += 100;
                if (noRecordsCount > giveUp) break;
            } else {
                noRecordsCount = 0;
                records.forEach(record -> processRecord(record, userComments, userLikes, userPopularity));
            }

            if (!records.isEmpty()) {
                try {
                    writeToJsonFiles(userComments, userLikes, userPopularity);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void processRecord(ConsumerRecord<String, String> record, Map<String, Set<String>> userComments, Map<String, Map<String, Integer>> userLikes, Map<String, Integer> userPopularity) {
        String[] parts = record.value().split(" ");
        String topic = record.topic();
        String userWhoPosted = parts[1];
        String postId = parts[2];

        switch (topic) {
            case "likes":
                userLikes.computeIfAbsent(userWhoPosted, k -> new HashMap<>())
                         .merge(postId, 1, Integer::sum);
                userPopularity.merge(userWhoPosted, 1, Integer::sum);
                break;
            case "comments":
                userComments.computeIfAbsent(userWhoPosted, k -> new HashSet<>()).add(parts[3]);
                userPopularity.merge(userWhoPosted, 5, Integer::sum);
                break;
            case "shares":
                int shareCount = parts.length - 3;
                userPopularity.merge(userWhoPosted, 20 * shareCount, Integer::sum);  //compute popularity
                break;
        }
    }
    
    //output Json file
    private static void writeToJsonFiles(Map<String, Set<String>> userComments, Map<String, Map<String, Integer>> userLikes, Map<String, Integer> userPopularity) throws Exception {
        try (FileWriter commentsWriter = new FileWriter("comments.json");
             FileWriter likesWriter = new FileWriter("likes.json");
             FileWriter popularityWriter = new FileWriter("popularity.json")) {

            mapper.writeValue(commentsWriter, userComments);
            mapper.writeValue(likesWriter, userLikes);

            ObjectNode popularityJson = mapper.createObjectNode();
            userPopularity.forEach((user, popularity) -> {
                popularityJson.put(user, popularity / 1000.0);
            });
            mapper.writeValue(popularityWriter, popularityJson);
        }
    }

    public static void main(String[] args) {
        Properties props = loadConsumerProperties();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList("likes", "comments", "shares"));
            processMessages(consumer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
