package com.synechisveltiosi.inventoryservice.infrastructure;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaRecoveryTest {
    @Test
    @SuppressWarnings("unchecked")
    void recoveryWaitsForAcknowledgementAndPreservesRecord() throws Exception {
        var kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        var acknowledged = new CompletableFuture<SendResult<String, String>>();
        var sent = new CountDownLatch(1);
        when(kafka.send(any(ProducerRecord.class))).thenAnswer(call -> {
            ProducerRecord<String, String> output = call.getArgument(0);
            assertEquals("order.events.inventory-service.DLT", output.topic());
            assertEquals(2, output.partition());
            assertEquals("identity", output.key());
            assertEquals("original bytes", output.value());
            assertEquals(3, output.headers().toArray().length);
            sent.countDown();
            return acknowledged;
        });
        var metrics = new SimpleMeterRegistry();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var result = executor.submit(() -> new KafkaConfiguration().recover(kafka, metrics,
                    new ConsumerRecord<>("order.events", 2, 7, "identity", "original bytes")));
            assertTrue(sent.await(2, TimeUnit.SECONDS));
            assertFalse(result.isDone());
            acknowledged.complete(null);
            result.get(2, TimeUnit.SECONDS);
            assertEquals(1, metrics.counter("consumer.dlt.published").count());
        } finally {
            metrics.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedAcknowledgementCannotRecoverSourceRecord() {
        var kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("private diagnostic")));
        var metrics = new SimpleMeterRegistry();
        try {
            var error = assertThrows(IllegalStateException.class, () -> new KafkaConfiguration().recover(kafka, metrics,
                    new ConsumerRecord<>("order.events", 0, 1, "identity", "payload")));
            assertFalse(error.getMessage().contains("private diagnostic"));
            assertNull(error.getCause());
            assertEquals(0, metrics.counter("consumer.dlt.published").count());
            assertEquals(1, metrics.counter("consumer.dlt.failures").count());
        } finally {
            metrics.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void errorHandlerDoesNotRecoverAfterFailedSend() {
        var kafka = (KafkaTemplate<String, String>) mock(KafkaTemplate.class);
        when(kafka.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        var metrics = new SimpleMeterRegistry();
        try {
            var handler = new KafkaConfiguration().inventoryErrorHandler(kafka, metrics);
            assertFalse(handler.handleOne(new IllegalArgumentException("Invalid envelope"),
                    new ConsumerRecord<>("order.events", 0, 1, "identity", "payload"),
                    mock(org.apache.kafka.clients.consumer.Consumer.class), mock(org.springframework.kafka.listener.MessageListenerContainer.class)));
        } finally {
            metrics.close();
        }
    }
}
