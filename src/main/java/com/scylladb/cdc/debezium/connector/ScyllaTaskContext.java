package com.scylladb.cdc.debezium.connector;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.schema.DataCollectionId;

import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.function.Supplier;

public class ScyllaTaskContext extends CdcSourceTaskContext {

    private String[] streamIds;
    private Date generationStart;

    public ScyllaTaskContext(Configuration config, String[] streamIds, Date generationStart) {
        super("workInProgress",  "workInProgress", Collections::emptySet);
        this.streamIds = streamIds;
        this.generationStart = generationStart;
    }

    public String[] streamIds() {
        return streamIds;
    }

    public Date generationStart() {
        return generationStart;
    }
}
