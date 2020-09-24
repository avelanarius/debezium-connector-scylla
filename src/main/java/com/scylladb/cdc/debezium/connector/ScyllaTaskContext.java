package com.scylladb.cdc.debezium.connector;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.schema.DataCollectionId;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

public class ScyllaTaskContext extends CdcSourceTaskContext {

    private String[] streamIds;

    public ScyllaTaskContext(Configuration config, String[] streamIds) {
        super("workInProgress",  "workInProgress", Collections::emptySet);
        this.streamIds = streamIds;
    }

    public String[] streamIds() {
        return streamIds;
    }
}
