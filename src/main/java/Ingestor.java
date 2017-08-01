import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Ingestor {

    private static final Gson gson = new Gson();
    private static final Random random = new Random();
    private static final String key = "msg";
    private static final String rawMsgKafkaTopic = System.getenv("RAW_MSG_TOPIC");
    private static final KafkaProducer<String, String> producer = initProducer();
    private static final ScheduledExecutorService msgPushService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat
            ("spark-stateful-example-data-generator-".concat("-%d")).build());

    public static void main(String[] args) {

        try {
            runIngestion();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // prevent app from crashing - hold on jvm with user thread
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        // registering the shutdown hook
        Runtime.getRuntime().addShutdownHook(new ShutDownJVM(countDownLatch));

        // wait indefinitely for the application to be stopped externally -- idle waiting
        executorService.execute(() -> {
            System.out.println("Waiting indefinitely for stop signal on the application");
            try {
                countDownLatch.await();
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Application Shutting Down");
        });

        System.out.println("Main Thread Completed - JVM held on user thread");
    }

    public static void runIngestion() throws Exception {
        msgPushService.scheduleAtFixedRate(() -> {
            try {
                pushData(false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private static void pushData(final boolean isMsgLast) throws IOException {
        producer.send(new ProducerRecord<>(rawMsgKafkaTopic, key, getData(isMsgLast)), (recordMetadata, e) -> {
            System.out.println("======" + key + "========");
            if (e != null) {
                System.out.println("ERROR in sending message" + e);
            } else {
                System.out.println("Message sent to topic : " + recordMetadata.topic() + " at offset " + recordMetadata.offset());
            }
        });
    }

    private static KafkaProducer<String, String> initProducer() {
        //Creating Producer
        final VcapServices vcapServices = VcapServices.getInstance().orElseThrow(() -> new RuntimeException("No VCAP Services available"));
        final String brokerConnectionString = jsonArrayToConnection(vcapServices.getCredentialsByName("kafka-service")
                .get("brokers").getAsJsonArray());
        Properties producerProperties = new Properties();
        producerProperties.put("client.id", "spark-stateful-example");
        producerProperties.put("bootstrap.servers", brokerConnectionString);
        producerProperties.put("key.serializer", StringSerializer.class);
        producerProperties.put("value.serializer", StringSerializer.class);
        return (KafkaProducer<String, String>) new KafkaProducer(producerProperties);
    }

    public static String getData(final boolean isMsgLast) throws IOException {
        return gson.toJson("" +
                "{" +
                    "\"id\":" + random.nextInt(5) + "," +
                    "\"data\":" + "\"" + Integer.toHexString(random.nextInt()) + "\"," +
                    "\"isLast\":" + String.valueOf(isMsgLast) +
                "}");
    }

    public static String jsonArrayToConnection(final JsonArray array) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < array.size(); i++) {
            final JsonObject element = array.get(i).getAsJsonObject();
            if (i > 0) sb.append(",");
            sb.append(element.get("hostname").getAsString()).append(":").append(element.get("port").getAsInt());
        }
        return sb.toString();
    }

    static class ShutDownJVM extends Thread {

        private final CountDownLatch countDownLatch;

        ShutDownJVM(final CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            System.out.println("Shutdown Signal Received");
            try {
                msgPushService.shutdown();
                pushData(true);
                producer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            countDownLatch.countDown();
        }
    }

}
