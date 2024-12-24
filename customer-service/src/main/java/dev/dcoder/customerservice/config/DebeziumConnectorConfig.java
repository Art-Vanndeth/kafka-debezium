package dev.dcoder.customerservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

@Configuration
public class DebeziumConnectorConfig {

    @Value("${debezium.database.hostname}")
    private String databaseHostname;

    @Value("${debezium.database.port}")
    private String databasePort;

    @Value("${debezium.database.user}")
    private String databaseUser;

    @Value("${debezium.database.password}")
    private String databasePassword;

    @Value("${debezium.database.dbname}")
    private String databaseName;

    @Bean
    public io.debezium.config.Configuration customerConnector() {
        return io.debezium.config.Configuration.create()
                .with("name", "customer-connector")
                .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("tasks.max", "1")
                .with("database.hostname", databaseHostname)
                .with("database.port", databasePort)
                .with("database.user", databaseUser)
                .with("database.password", databasePassword)
                .with("database.dbname", databaseName)
                .with("database.server.name", "source")
                .with("schema.include.list", "public")
                .with("plugin.name", "pgoutput")
                .with("slot.name", "debezium_slot")
                .with("publication.name", "debezium_pub")
                .with("table.include.list", "public.customer")
                .with("database.history.kafka.bootstrap.servers", "kafka:9092")
                .with("database.history.kafka.topic", "schema-changes.customer")
                .with("topic.prefix", "source")
                .with("tombstones.on.delete", "false")
                .with("key.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("key.converter.schemas.enable", "true")
                .with("value.converter", "org.apache.kafka.connect.json.JsonConverter")
                .with("value.converter.schemas.enable", "true")
                .with("heartbeat.interval.ms", "5000")
                .with("snapshot.mode", "initial")
                .build();
    }
}

