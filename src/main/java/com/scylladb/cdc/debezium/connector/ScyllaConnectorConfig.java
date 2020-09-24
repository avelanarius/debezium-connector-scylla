package com.scylladb.cdc.debezium.connector;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import org.apache.kafka.common.config.ConfigDef;

public class ScyllaConnectorConfig extends CommonConnectorConfig {

    public static final Field STREAM_IDS = Field.create("scylla.stream.ids")
            .withDisplayName("Stream IDs")
            .withType(ConfigDef.Type.LIST)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH);
    // workInProgress .withValidation(MongoDbConnectorConfig::validateHosts)
    // workInProgress .withDescription("The hostname and port pairs (in the form 'host' or 'host:port') "
    // workInProgress        + "of the MongoDB server(s) in the replica set.");

    private static final ConfigDefinition CONFIG_DEFINITION =
            CommonConnectorConfig.CONFIG_DEFINITION.edit()
                    .name("Scylla")
                    .type(STREAM_IDS)
                    .create();

    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    protected ScyllaConnectorConfig(Configuration config) {
        super(config, "workInProgress logical name", 0);
    }

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    @Override
    public String getContextName() {
        return "Scylla";
    }

    @Override
    public String getConnectorName() {
        return "scylla";
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new ScyllaSourceInfoStructMaker("scylla", "VERSION", this);
    }
}
