import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** One explicit DLT position, no group commits, byte-preserving replay, dry-run by default. */
class Redrive {
    private static final Map<String, Set<String>> SOURCES = Map.of(
            "order-service", Set.of("inventory.events", "payment.events", "shipping.events"),
            "inventory-service", Set.of("order.events", "payment.events"),
            "payment-service", Set.of("inventory.events", "shipping.events"),
            "shipping-service", Set.of("payment.events"),
            "notification-service", Set.of("order.events"),
            "order-query-service", Set.of("order.events", "inventory.events", "payment.events", "shipping.events", "notification.events"));

    public static void main(String[] args) throws Exception {
        if (args.length < 4 || args.length > 5 || (args.length == 5 && !args[4].equals("--execute")))
            throw new IllegalArgumentException("Usage: redrive.py BOOTSTRAP DLT_TOPIC PARTITION OFFSET [--execute]");
        String dlt = args[1], source = null;
        for (var entry : SOURCES.entrySet()) for (String topic : entry.getValue())
            if (dlt.equals(topic + "." + entry.getKey() + ".DLT")) source = topic;
        if (source == null) throw new IllegalArgumentException("Unknown consumer-specific DLT");
        int partition = Integer.parseInt(args[2]); long offset = Long.parseLong(args[3]);
        if (partition < 0 || offset < 0) throw new IllegalArgumentException("Partition and offset must be nonnegative");
        var props = new Properties(); props.put("bootstrap.servers", args[0]);
        props.put("enable.auto.commit", "false"); props.put("auto.offset.reset", "none");
        props.put("isolation.level", "read_committed"); props.put("default.api.timeout.ms", "10000");
        props.put("allow.auto.create.topics", "false");
        ConsumerRecord<byte[], byte[]> selected = null;
        var position = new TopicPartition(dlt, partition);
        try (var consumer = new KafkaConsumer<byte[], byte[]>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer.assign(List.of(position)); consumer.seek(position, offset);
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (selected == null && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(250))) {
                    if (record.offset() == offset) { selected = record; break; }
                    if (record.offset() > offset) throw new IllegalArgumentException("Requested offset is no longer retained");
                }
            }
        }
        if (selected == null) throw new IllegalArgumentException("No record at the requested offset");
        System.out.println("Selected " + dlt + " partition " + partition + " offset " + offset + "; destination " + source);
        if (args.length == 4) { System.out.println("Dry run; no record published. Add --execute after fixing the cause."); return; }
        props = new Properties(); props.put("bootstrap.servers", args[0]); props.put("acks", "all");
        props.put("enable.idempotence", "true"); props.put("max.block.ms", "10000");
        props.put("request.timeout.ms", "5000"); props.put("delivery.timeout.ms", "10000");
        try (var producer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            var result = producer.send(new ProducerRecord<>(source, partition, selected.key(), selected.value())).get(12, TimeUnit.SECONDS);
            System.out.println("Acknowledged replay at " + result.topic() + " partition " + result.partition() + " offset " + result.offset());
        }
    }
}
