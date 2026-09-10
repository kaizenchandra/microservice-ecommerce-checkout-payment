package com.synechisveltiosi.orderqueryservice.infrastructure;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import java.util.concurrent.ThreadLocalRandom;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Configuration
public class KafkaConfiguration {
    static final String DLT_SUFFIX = ".order-query-service.DLT";

    @Bean
    KafkaAdmin.NewTopics deadLetterTopics() {
        return new KafkaAdmin.NewTopics(TopicBuilder.name("order.events" + DLT_SUFFIX).partitions(3).replicas(1).config("retention.ms", "604800000").build(),
                TopicBuilder.name("inventory.events" + DLT_SUFFIX).partitions(3).replicas(1).config("retention.ms", "604800000").build(),
                TopicBuilder.name("payment.events" + DLT_SUFFIX).partitions(3).replicas(1).config("retention.ms", "604800000").build(),
                TopicBuilder.name("shipping.events" + DLT_SUFFIX).partitions(3).replicas(1).config("retention.ms", "604800000").build(),
                TopicBuilder.name("notification.events" + DLT_SUFFIX).partitions(3).replicas(1).config("retention.ms", "604800000").build());
    }

    @Bean
    DefaultErrorHandler projectionErrorHandler(KafkaTemplate<String, String> kafka, MeterRegistry metrics) {
        var backoff = new ExponentialBackOff(250, 2);
        backoff.setMaxInterval(1000);
        backoff.setMaxAttempts(3);
        BackOff jittered = () -> {
            var execution = backoff.start();
            return () -> {
                long delay = execution.nextBackOff();
                return delay == BackOffExecution.STOP ? delay : ThreadLocalRandom.current().nextLong(delay / 2, delay + 1);
            };
        };
        var handler = new DefaultErrorHandler((record, failure) -> recover(kafka, metrics, record), jittered);
        // Invalid envelopes cannot be repaired by retrying the same bytes.
        handler.setClassifications(Map.of(IllegalArgumentException.class, false), true);
        return handler;
    }

    void recover(KafkaTemplate<String, String> kafka, MeterRegistry metrics, ConsumerRecord<?, ?> record) {
        // Deliberately omit exception messages and stack traces from recovery headers.
        var headers = new RecordHeaders();
        headers.add("original-topic", record.topic().getBytes(StandardCharsets.UTF_8));
        headers.add("original-partition", Integer.toString(record.partition()).getBytes(StandardCharsets.UTF_8));
        headers.add("original-offset", Long.toString(record.offset()).getBytes(StandardCharsets.UTF_8));
        var output = new ProducerRecord<String, String>(record.topic() + DLT_SUFFIX, record.partition(),
                record.timestamp() < 0 ? null : record.timestamp(), (String) record.key(), (String) record.value(), headers);
        try {
            kafka.send(output).get(6, TimeUnit.SECONDS);
            metrics.counter("consumer.dlt.published").increment();
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            metrics.counter("consumer.dlt.failures").increment();
            throw new IllegalStateException("Dead-letter publication was not acknowledged");
        } catch (Exception failure) {
            metrics.counter("consumer.dlt.failures").increment();
            // Throwing keeps the source offset unacknowledged; recovery is retried.
            throw new IllegalStateException("Dead-letter publication was not acknowledged");
        }
    }
}
