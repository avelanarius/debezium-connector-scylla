1. Code written most sloppily/requiring writing it properly marked with `workInProgress` or `TODO`.

2. Determining task definitions from master node:

https://github.com/avelanarius/debezium-connector-scylla/blob/e49d9d23418e9957d482238879836c75fcac0c7e/src/main/java/com/scylladb/cdc/debezium/connector/ScyllaConnector.java#L61-L141

3. Worker "work" loop:

https://github.com/avelanarius/debezium-connector-scylla/blob/e49d9d23418e9957d482238879836c75fcac0c7e/src/main/java/com/scylladb/cdc/debezium/connector/ScyllaStreamingChangeEventSource.java#L37-L78

4. How are "worker" offsets represented (offset per VNode, it probably should be offset per VNode&GenerationTime):

https://github.com/avelanarius/debezium-connector-scylla/blob/e49d9d23418e9957d482238879836c75fcac0c7e/src/main/java/com/scylladb/cdc/debezium/connector/SourceInfo.java#L35-L38
