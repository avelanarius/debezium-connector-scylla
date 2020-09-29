package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.debezium.connector.tmpclient.StreamIdsProvider;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Collect;
import io.debezium.util.Metronome;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class ScyllaStreamingChangeEventSource implements StreamingChangeEventSource {

    private final ScyllaConnectorConfig configuration;
    private ScyllaTaskContext taskContext;
    private final ScyllaOffsetContext offsetContext;
    private final EventDispatcher<CollectionId> dispatcher;
    private final Clock clock;
    private final Duration pollInterval;

    public ScyllaStreamingChangeEventSource(ScyllaConnectorConfig configuration, ScyllaTaskContext taskContext, ScyllaOffsetContext offsetContext, EventDispatcher<CollectionId> dispatcher, Clock clock) {
        this.configuration = configuration;
        this.taskContext = taskContext;
        this.offsetContext = offsetContext;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.pollInterval = configuration.getPollInterval();
    }

    @Override
    public void execute(ChangeEventSourceContext context) throws InterruptedException {
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);

        while (context.isRunning()) {
            // workInProgress

            boolean noNewData = true;

            Cluster cluster = Cluster.builder().addContactPoints("127.0.0.2").withPort(9042).build();
            Session session = cluster.connect();

            Map<Long, List<String>> streamsByVNodes = StreamIdsProvider.splitStreamIdsByVNodesMap(Arrays.asList(taskContext.streamIds()));

            for (Map.Entry<Long, List<String>> entry : streamsByVNodes.entrySet()) {
                VNodeOffsetContext vNodeOffsetContext = offsetContext.vnodeOffsetContext(entry.getKey(), taskContext.generationStart());
                UUID lastOffset = vNodeOffsetContext.lastOffsetUUID();
                Date windowEndDate = Date.from(Instant.now().minusSeconds(10));
                UUID windowEnd = UUIDs.endOf(windowEndDate.getTime());

                // workInProgress FIXME do one batch query - very important as this one is not fully correct!!!
                //  (because we are not reading in sorted order by time)
                for (String streamId : entry.getValue()) {
                    String queryString = "SELECT * FROM ks.t_scylla_cdc_log WHERE \"cdc$stream_id\" = " + streamId
                            + " AND \"cdc$time\" > " + lastOffset.toString() + " AND \"cdc$time\" < " + windowEnd;
                    ResultSet rs = session.execute(queryString);
                    for (Row r : rs) {
                        noNewData = false;

                        UUID time = r.get("cdc$time", UUID.class);
                        vNodeOffsetContext.dataChangeEvent(time);
                        dispatcher.dispatchDataChangeEvent(new CollectionId(),
                                new ScyllaChangeRecordEmitter(r, vNodeOffsetContext, clock));
                    }
                }

                // Move window
                vNodeOffsetContext.dataChangeEvent(windowEnd);
                dispatcher.alwaysDispatchHeartbeatEvent(vNodeOffsetContext);
            }

            session.close();
            cluster.close();

            if (noNewData) {
                metronome.pause();
            }
        }
    }
}
