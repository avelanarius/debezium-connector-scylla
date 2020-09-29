package com.scylladb.cdc.debezium.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.UUIDs;
import com.scylladb.cdc.Generation;
import com.scylladb.cdc.debezium.connector.tmpclient.StreamIdsProvider;
import com.scylladb.cdc.driver.Reader;
import com.scylladb.cdc.master.GenerationsFetcher;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ScyllaConnector extends SourceConnector {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private Map<String, String> props;
    private Configuration config;
    private Thread workInProgressRefreshThread;

    public ScyllaConnector() {
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;

        final Configuration config = Configuration.from(props);
        this.config = config;

        this.workInProgressRefreshThread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(20000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    ScyllaConnector.this.context.requestTaskReconfiguration();
                }
            }
        });
        this.workInProgressRefreshThread.start();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ScyllaConnectorTask.class;
    }

    private Date lastDate = new Date(0);

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // workInProgress
        // Preliminary "algorithm"

        Cluster cluster = Cluster.builder().addContactPoints("127.0.0.2").withPort(9042).build();
        Session session = cluster.connect();

        final ScyllaConnectorConfig connectorConfig = new ScyllaConnectorConfig(config);
        SourceInfo sourceInfo = new SourceInfo(connectorConfig);

        Reader<Date> tReader = Reader.createGenerationsTimestampsReader(session);
        Reader<Set<ByteBuffer>> gsReader = Reader.createGenerationStreamsReader(session);
        GenerationsFetcher fetcher = new GenerationsFetcher(tReader, gsReader);

        Generation currentGen = null;
        try {
            currentGen = fetcher.fetchNext(lastDate, new AtomicBoolean(true)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        if (currentGen == null) {
            return Collections.emptyList();
        }

        if (!currentGen.metadata.endTimestamp.isPresent()) {
            // Current generation is still open, work on it
        } else {
            // If generation is closed, check if all
            // workers read it at least up to (generationEnd + SAFETY_WINDOW).
            Map<Long, List<String>> splitByVNodes = StreamIdsProvider.splitStreamIdsByVNodesMap(
                    currentGen.streamIds.stream().map(q -> "0x" + q.toString()).collect(Collectors.toList()));
            Date generationEndWithSafety = Date.from(currentGen.metadata.endTimestamp.get().toInstant().plusSeconds(30));

            boolean allWorkersFinished = true;
            for (Map.Entry<Long, List<String>> entry : splitByVNodes.entrySet()) {
                Map<String, Object> readOffset = context().offsetStorageReader().offset(sourceInfo.partition(entry.getKey(), currentGen.metadata.startTimestamp));
                UUID lastOffsetUUID = UUIDs.startOf(0);
                if (readOffset != null) {
                    lastOffsetUUID = UUID.fromString((String) readOffset.get(SourceInfo.OFFSET));
                }

                Date lastOffsetDate = new Date(UUIDs.unixTimestamp(lastOffsetUUID));
                if (lastOffsetDate.before(generationEndWithSafety)) {
                    allWorkersFinished = false;
                    logger.warn("Worker not finished in: " + currentGen.metadata.startTimestamp + " " + lastOffsetDate + " < " + generationEndWithSafety);
                    break;
                }
            }

            if (allWorkersFinished) {
                // All workers finished (past generationEnd + SAFETY_WINDOW).
                // Move to next gen!
                lastDate = currentGen.metadata.startTimestamp;
                return taskConfigs(maxTasks);
            } else {
                // Still work on current generation
            }
        }

        session.close();
        cluster.close();

        List<Map<String, String>> tasks = new ArrayList<>();

        Collection<List<String>> streamIdsList = StreamIdsProvider.splitStreamIdsByVNodes(currentGen.streamIds.stream().map(q -> "0x" + q.toString()).collect(Collectors.toList()));
        Collection<List<String>> chunks = StreamIdsProvider.reduceCount(streamIdsList, 5);

        for (List<String> chunk : chunks) {
            Map<String, String> taskConfig = config.edit()
                    .with(ScyllaConnectorConfig.STREAM_IDS, String.join(",", chunk))
                    .with(ScyllaConnectorConfig.GENERATION_START, Long.toString(currentGen.metadata.startTimestamp.getTime()))
                    .build().asMap();
            tasks.add(taskConfig);
        }

        System.out.println("Will queue up: " + tasks.size());
        return tasks;
    }

    @Override
    public void stop() {
        this.workInProgressRefreshThread.stop();
    }

    @Override
    public String version() {
        return "workInProgress";
    }

    @Override
    public ConfigDef config() {
        return ScyllaConnectorConfig.configDef();
    }
}
