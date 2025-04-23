package com.bencejdanko.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlidingWindowStreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowStreamingJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink DGIM Sliding Window Job Setup...");

        try {
            // 1. Set up the Flink execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            final EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            LOG.info("Flink Execution Environment and Table Environment created.");

            // --- Configuration ---
            final String kafkaBootstrapServers = "kafka:9093";
            final String sourceTopic = "network_traffic";
            final String slidingSinkTopic = "sliding_window_output"
            final String consumerGroupId = "flink-dgim-sliding-group-java";
            final long slidingWindowSeconds = 60L;
            final long slidingWindowSlideSeconds = 10L;

            // 2. Define Kafka Source Table DDL
            final String sourceDDL = String.format(
                "CREATE TABLE kafka_source (" +
                "  `value` STRING,       " +
                "  `timestamp` STRING,   " +
                "  proctime AS PROCTIME()" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'properties.group.id' = '%s'," +
                "  'scan.startup.mode' = 'latest-offset'," +
                "  'format' = 'json'," +
                "  'json.fail-on-missing-field' = 'false'," +
                "  'json.ignore-parse-errors' = 'true'" +
                ")", sourceTopic, kafkaBootstrapServers, consumerGroupId
            );

            LOG.info("Creating Kafka source table with DDL:\n{}", sourceDDL);
            tEnv.executeSql(sourceDDL);
            LOG.info("Kafka source table '{}' created.", sourceTopic);

            // 3. Define Kafka Sink Table DDL for Sliding Window Output
            final String slidingSinkDDL = String.format(
                "CREATE TABLE kafka_sliding_sink (" +
                "  window_start STRING," +    // Window start time
                "  window_end STRING," +      // Window end time
                "  count_estimate BIGINT," +   // DGIM count estimate
                // Sliding windows often overlap, so a composite key is safer for upsert
                "  PRIMARY KEY (window_start, window_end) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'key.format' = 'json'," +
                "  'value.format' = 'json'" +
                ")", slidingSinkTopic, kafkaBootstrapServers // Use the new sink topic
            );

            LOG.info("Creating Kafka sliding sink table with DDL:\n{}", slidingSinkDDL);
            tEnv.executeSql(slidingSinkDDL);
            LOG.info("Kafka sink table '{}' created.", slidingSinkTopic);

            // 4. Register the DGIM Aggregate Function (Same function, different instantiation context)
            tEnv.createTemporarySystemFunction("DGIM_ESTIMATE", new DGIMAggregateFunction(slidingWindowSeconds));
            LOG.info("DGIM Aggregate Function registered with effective window size of {} seconds for internal expiry.", slidingWindowSeconds);

            // 5. Define the SLIDING windowed aggregation SQL using HOP function
            final String insertSQL = String.format(
                "INSERT INTO kafka_sliding_sink " + // Target the new sink table
                "SELECT " +
                "  CAST(window_start AS STRING) AS window_start, " + // Include window_start
                "  CAST(window_end AS STRING) AS window_end, " +
                "  DGIM_ESTIMATE(" +
                "    `value`, " +
                "    TRY_CAST(UNIX_TIMESTAMP(CAST(`timestamp` AS STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS') AS BIGINT)" +
                "  ) AS count_estimate " +
                "FROM TABLE(" +
                // Use HOP for sliding windows: HOP(TABLE table, DESCRIPTOR(timecol), slide_interval, size_interval)
                "  HOP(TABLE kafka_source, DESCRIPTOR(proctime), INTERVAL '%d' SECOND, INTERVAL '%d' SECOND)" +
                ") " +
                "WHERE `value` IS NOT NULL AND TRY_CAST(UNIX_TIMESTAMP(CAST(`timestamp` AS STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS') AS BIGINT) IS NOT NULL " +
                "GROUP BY window_start, window_end",
                slidingWindowSlideSeconds,
                slidingWindowSeconds
            );

            LOG.info("Submitting INSERT INTO statement with DGIM sliding window aggregation:\n{}", insertSQL);
            tEnv.executeSql(insertSQL);
            LOG.info("SQL query submitted for execution plan generation.");

            // Do NOT call env.execute() here! Table API SQL jobs are managed by executeSql.

        } catch (Exception e) {
            LOG.error("An error occurred during Flink sliding window job execution:", e);
            // Re-throw or handle appropriately
            throw e;
        }
    }
}