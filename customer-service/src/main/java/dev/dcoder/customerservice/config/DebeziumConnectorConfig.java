package dev.dcoder.customerservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DebeziumConnectorConfig {

    @Value("${debezium.name}")
    private String connectorName;

    @Value("${debezium.connector.class}")
    private String connectorClass;

    @Value("${debezium.tasks.max}")
    private String tasksMax;

    @Value("${debezium.database.hostname}")
    private String databaseHostname;

    @Value("${debezium.database.port}")
    private String databasePort;

    @Value("${debezium.database.user}")
    private String databaseUser;

    @Value("${debezium.database.password}")
    private String databasePassword;

    @Value("${debezium.database.dbname}")
    private String databaseDbname;

    @Value("${debezium.database.server-name}")
    private String databaseServerName;

    @Value("${debezium.plugin.name}")
    private String pluginName;

    @Value("${debezium.slot.name}")
    private String slotName;

    @Value("${debezium.publication.name}")
    private String publicationName;

    @Value("${debezium.tables.include-list}")
    private String tableIncludeList;

    @Value("${debezium.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${debezium.kafka.topic.schema-changes}")
    private String schemaChangesTopic;

    @Value("${debezium.kafka.topic.prefix}")
    private String topicPrefix;

    @Value("${debezium.schema.include-list}")
    private String schemaIncludeList;

    @Bean
    public io.debezium.config.Configuration customerConnector() {
        return io.debezium.config.Configuration.create()
                .with("name", connectorName)
                .with("connector.class", connectorClass)
                .with("tasks.max", tasksMax)
                .with("database.hostname", databaseHostname)
                .with("database.port", databasePort)
                .with("database.user", databaseUser)
                .with("database.password", databasePassword)
                .with("database.dbname", databaseDbname)
                .with("database.server.name", databaseServerName)
                .with("schema.include.list", schemaIncludeList)
                .with("plugin.name", pluginName)
                .with("slot.name", slotName)
                .with("publication.name", publicationName)
                .with("table.include.list", tableIncludeList)
                .with("database.history.kafka.bootstrap.servers", kafkaBootstrapServers)
                .with("database.history.kafka.topic", schemaChangesTopic)
                .with("topic.prefix", topicPrefix)
                .build();
    }
}

