package dev.dcoder.syncservice.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dcoder.syncservice.model.Customer;
import dev.dcoder.syncservice.repository.CustomerRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

@Service
public class CustomerSyncListener {
    private static final Logger log = LoggerFactory.getLogger(CustomerSyncListener.class);

    private final CustomerRepository customerRepository;
    private final ObjectMapper objectMapper;

    // Enum for CDC operations
    private enum Operation {
        CREATE("c"),
        UPDATE("u"),
        DELETE("d");

        private final String code;

        Operation(String code) {
            this.code = code;
        }

        static Optional<Operation> fromCode(String code) {
            return Stream.of(values())
                    .filter(op -> op.code.equals(code))
                    .findFirst();
        }
    }

    public CustomerSyncListener(CustomerRepository customerRepository, ObjectMapper objectMapper) {
        this.customerRepository = customerRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "mytopic.public.customer", groupId = "sync-service-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            processRecord(record)
                    .ifPresent(this::handleOperation);
        } catch (Exception e) {
            log.error("Error processing Kafka record: {}", record.value(), e);
        }
    }

    private Optional<ProcessedRecord> processRecord(ConsumerRecord<String, String> record) {
        return Optional.ofNullable(record.value())
                .filter(value -> !value.isEmpty())
                .map(this::parseJson)
                .map(this::extractProcessedRecord);
    }

    private JsonNode parseJson(String value) {
        try {
            return objectMapper.readTree(value);
        } catch (Exception e) {
            log.error("Failed to parse JSON: {}", value, e);
            throw new RuntimeException("JSON parsing failed", e);
        }
    }

    private ProcessedRecord extractProcessedRecord(JsonNode jsonNode) {
        JsonNode payload = jsonNode.path("payload");
        String operationCode = payload.path("op").asText();
        return new ProcessedRecord(
                Operation.fromCode(operationCode),
                payload.path("after"),
                payload.path("before")
        );
    }

    private void handleOperation(ProcessedRecord record) {
        record.operation.ifPresent(op -> {
            switch (op) {
                case CREATE, UPDATE -> handleUpsert(record.afterNode);
                case DELETE -> handleDelete(record.beforeNode);
                default -> log.warn("Unsupported operation: {}", op);
            }
        });
    }

    private void handleUpsert(JsonNode node) {
        if (!node.isMissingNode()) {
            Customer customer = extractCustomer(node);
            customerRepository.save(customer);
            log.info("Inserted Customer: {}", customer);
        }
    }

    private void handleDelete(JsonNode node) {
        if (!node.isMissingNode()) {
            Long id = node.path("id").asLong();
            customerRepository.deleteById(id);
            log.info("Deleted Customer with ID: {}", id);
        }
    }

    private Customer extractCustomer(JsonNode node) {
        return new Customer(
                node.path("id").asLong(),
                node.path("first_name").asText(),
                node.path("last_name").asText(),
                node.path("email").asText()
        );
    }

    private record ProcessedRecord(
            Optional<Operation> operation,
            JsonNode afterNode,
            JsonNode beforeNode
    ) {}
}