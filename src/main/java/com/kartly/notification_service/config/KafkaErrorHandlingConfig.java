package com.kartly.notification_service.config;

import com.kartly.notification_service.dto.OrderStatusEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            MeterRegistry meterRegistry) {

        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        DelegatingByTypeSerializer valueSerializer = new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(),
                OrderStatusEvent.class, new JacksonJsonSerializer<>()));

        KafkaTemplate<String, Object> dlqTemplate = new KafkaTemplate<>(
                new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer));

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dlqTemplate);

        ConsumerRecordRecoverer countingRecoverer = (record, exception) -> {
            recoverer.accept(record, exception);
            Counter.builder("kartly.dlt.messages")
                    .description("Messages parked in the dead letter topic")
                    .tag("source_topic", record.topic())
                    .register(meterRegistry)
                    .increment();
        };

        return new DefaultErrorHandler(countingRecoverer, new FixedBackOff(1000L, 3L));
    }
}