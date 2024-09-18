package dev.dcoder.syncservice.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.dcoder.syncservice.model.Customer;
import dev.dcoder.syncservice.repository.CustomerRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CustomerSyncListener {

    @Autowired
    private CustomerRepository customerRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "mytopic.public.customer", groupId = "sync-service-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            // Check if the message payload is not null or empty
            if (record.value() == null || record.value().isEmpty()) {
                System.err.println("Received null or empty message");
                return;
            }

            // Parse the JSON event received from Kafka
            JsonNode jsonNode = objectMapper.readTree(record.value());

            // Extract the 'op' (operation) field
            String operation = jsonNode.path("payload").path("op").asText();

            // Handle Insert/Update (using 'after') and Delete (using 'before')
            if ("c".equals(operation) || "u".equals(operation)) {
                // Insert or Update case - use 'after' field
                JsonNode afterNode = jsonNode.path("payload").path("after");
                if (!afterNode.isMissingNode()) {
                    // Map the 'after' field to a Customer entity
                    Long id = afterNode.path("id").asLong();
                    String firstName = afterNode.path("first_name").asText();
                    String lastName = afterNode.path("last_name").asText();
                    String email = afterNode.path("email").asText();

                    // Create or update the Customer in the sync-service database
                    Customer customer = new Customer(id, firstName, lastName, email);
                    customerRepository.save(customer);
                }

            } else if ("d".equals(operation)) {
                // Delete case - use 'before' field
                JsonNode beforeNode = jsonNode.path("payload").path("before");
                if (!beforeNode.isMissingNode()) {
                    // Extract ID from 'before' node
                    Long id = beforeNode.path("id").asLong();

                    // Delete the Customer from the sync-service database
                    customerRepository.deleteById(id);
                    System.out.println("Deleted Customer with ID: " + id);
                }
            } else {
                System.err.println("Unknown operation type: " + operation);
            }

        } catch (Exception e) {
            e.printStackTrace();  // Add proper logging here
        }
    }
}

