package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.utils.Bytes;
import com.scylladb.cdc.debezium.connector.tmpclient.StreamIdsProvider;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.stream.Collectors;

public class ScyllaConnectorTask extends BaseSourceTask {

    private static final String CONTEXT_NAME = "scylla-connector-task";

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private volatile ScyllaSchema schema;
    private volatile ScyllaTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile ErrorHandler errorHandler;

    @Override
    protected ChangeEventSourceCoordinator start(Configuration configuration) {
        System.out.println("[[Hello from start task!!!!!!!]]");

        final ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(configuration);
        final TopicSelector<CollectionId> topicSelector = TopicSelector.defaultSelector(connectorConfig,
                (id, prefix, delimiter) -> "quickstart-events");

        this.schema = new ScyllaSchema();

        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                .build();

        String[] streamIds = getStreamIds(configuration);

        this.taskContext = new ScyllaTaskContext(configuration, streamIds);

        final ScyllaEventMetadataProvider metadataProvider = new ScyllaEventMetadataProvider();

        final EventDispatcher<CollectionId> dispatcher = new EventDispatcher<CollectionId>(
                connectorConfig,
                topicSelector,
                schema,
                queue,
                (id) -> true,
                DataChangeEvent::new,
                metadataProvider,
                SchemaNameAdjuster.create(logger)
        );

        final ScyllaOffsetContext previousOffsets = getPreviousOffsets(connectorConfig, streamIds);

        this.errorHandler = new ScyllaErrorHandler("workInProgress", queue);

        final Clock clock = Clock.system();

        ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
                previousOffsets,
                errorHandler,
                ScyllaConnector.class,
                connectorConfig,
                new ScyllaChangeEventSourceFactory(connectorConfig, taskContext, dispatcher, clock),
                new DefaultChangeEventSourceMetricsFactory(),
                dispatcher,
                schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    private String[] getStreamIds(Configuration config) {
        String streamIds = config.getString(ScyllaConnectorConfig.STREAM_IDS);
        return streamIds.split(",");
    }

    private ScyllaOffsetContext getPreviousOffsets(ScyllaConnectorConfig connectorConfig, String[] streamIds) {
        SourceInfo sourceInfo = new SourceInfo(connectorConfig);
        Collection<Long> vNodes = StreamIdsProvider.splitStreamIdsByVNodesMap(Arrays.asList(streamIds)).keySet();
        for (long vnode : vNodes) {
            Map<String, String> partition = sourceInfo.partition(vnode);
            Map<String, Object> offset = context.offsetStorageReader().offset(partition);
            if (offset != null) {
                UUID lastOffsetUUID = UUID.fromString((String) offset.get(SourceInfo.OFFSET));
                sourceInfo.loadLastOffsetUUID(vnode, lastOffsetUUID);
            }
        }
        return new ScyllaOffsetContext(sourceInfo, new TransactionContext());
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        List<DataChangeEvent> records = queue.poll();
        return records.stream().map(DataChangeEvent::getRecord).collect(Collectors.toList());
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return ScyllaConnectorConfig.ALL_FIELDS;
    }

    @Override
    public String version() {
        return "workInProgress";
    }
}
