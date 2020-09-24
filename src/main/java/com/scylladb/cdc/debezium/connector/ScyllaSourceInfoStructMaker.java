package com.scylladb.cdc.debezium.connector;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class ScyllaSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public ScyllaSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("com.scylladb.cdc.debezium.connector")
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        return super.commonStruct(sourceInfo);
    }
}
