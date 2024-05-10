import org.apache.kafka.clients.producer.*;
import java.io.*;
import java.util.Properties;

public class SocialMediaProducer {
    private static Properties loadProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private static void sendMessages(Producer<String, String> producer, String inputFile) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ", 2);
                if (parts.length < 2) {
                    System.err.println("Skipping malformed line: " + line);
                    continue;
                }
                String topic = getTopic(parts[0]);
                if (topic != null) {
                    sendMessage(producer, topic, parts[1]);
                }
            }
        }
    }

    private static String getTopic(String messageType) {
    	//three topics
        switch (messageType.toLowerCase()) {
            case "like": return "likes";
            case "comment": return "comments";
            case "share": return "shares";
            default:
                System.err.println("Unknown message type: " + messageType);
                return null;
        }
    }

    private static void sendMessage(Producer<String, String> producer, String topic, String message) {
        producer.send(new ProducerRecord<>(topic, null, message), (metadata, exception) -> {
            if (exception != null) {
                System.err.println("Failed to send message: " + message + " to topic: " + topic);
                exception.printStackTrace();
            } else {
                System.out.println("Sent message: " + message + " to topic: " + topic);  //show message
            }
        });
    }

    public static void main(String[] args) {
        Properties props = loadProducerProperties();
        Producer<String, String> producer = new KafkaProducer<>(props);
        try {
            sendMessages(producer, "/home/hadoop/Documents/dataset/student_dataset.txt");  //data set path
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
