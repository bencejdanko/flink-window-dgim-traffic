package com.bencejdanko.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingJob {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink DGIM Window Job Setup...");

        try {
            // 1. Set up the Flink execution environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            // Use Streaming mode for Table API
            final EnvironmentSettings settings = EnvironmentSettings.newInstance()
                    .inStreamingMode()
                    .build();
            // Create the Table Environment
            final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            LOG.info("Flink Execution Environment and Table Environment created.");

            // --- Configuration ---
            final String kafkaBootstrapServers = "kafka:9093"; // Make sure this is correct and reachable!
            final String sourceTopic = "network_traffic";
            final String sinkTopic = "tumble_window_output";
            final String consumerGroupId = "flink-dgim-group-java";
            final long windowSeconds = 60L; // 60-second window

            // 2. Define Kafka Source Table DDL
            final String sourceDDL = String.format(
                "CREATE TABLE kafka_source (" +
                "  `value` STRING,       " + // Define the columns matching your JSON structure
                "  `timestamp` STRING,   " +
                "  proctime AS PROCTIME()" + // Add processing time for windowing
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'properties.group.id' = '%s'," +
                "  'scan.startup.mode' = 'latest-offset'," + // Or 'earliest-offset'
                "  'format' = 'json'," + // Use Flink's built-in JSON format
                "  'json.fail-on-missing-field' = 'false'," + // Tolerate missing fields
                "  'json.ignore-parse-errors' = 'true'" +     // Tolerate messages that aren't valid JSON
                ")", sourceTopic, kafkaBootstrapServers, consumerGroupId
            );

            LOG.info("Creating Kafka source table with DDL:\n{}", sourceDDL);
            tEnv.executeSql(sourceDDL);
            LOG.info("Kafka source table '{}' created.", sourceTopic);

            // 3. Define Kafka Sink Table DDL with updated schema
            final String sinkDDL = String.format(
                "CREATE TABLE kafka_sink (" +
                "  window_end STRING," +
                "  count_estimate BIGINT," + 
                "  PRIMARY KEY (window_end) NOT ENFORCED" + // Define window_end as the primary key
                ") WITH (" +
                "  'connector' = 'upsert-kafka'," +
                "  'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'key.format' = 'json'," +   // Format for the key part
                "  'value.format' = 'json'" +  // Format for the value part
                ")", sinkTopic, kafkaBootstrapServers
            );

            LOG.info("Creating Kafka sink table with DDL:\n{}", sinkDDL);
            tEnv.executeSql(sinkDDL);
            LOG.info("Kafka sink table '{}' created.", sinkTopic);

            // 4. Register the DGIM Aggregate Function
            tEnv.createTemporarySystemFunction("DGIM_ESTIMATE", new DGIMAggregateFunction(windowSeconds));
            LOG.info("DGIM Aggregate Function registered with window size of {} seconds.", windowSeconds);

            // 5. Define the windowed aggregation SQL
            final String insertSQL =
                "INSERT INTO kafka_sink " +
                "SELECT " +
                "  CAST(window_end AS STRING) AS window_end, " +
                "  DGIM_ESTIMATE(" +
                "    `value`, " +
                "    TRY_CAST(UNIX_TIMESTAMP(CAST(`timestamp` AS STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS') AS BIGINT)" +
                "  ) AS count_estimate " +
                "FROM TABLE(" +
                "  TUMBLE(TABLE kafka_source, DESCRIPTOR(proctime), INTERVAL '60' SECOND)" +
                ") " +
                "WHERE `value` IS NOT NULL AND TRY_CAST(UNIX_TIMESTAMP(CAST(`timestamp` AS STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS') AS BIGINT) IS NOT NULL " +
                "GROUP BY window_start, window_end";

            LOG.info("Submitting INSERT INTO statement with DGIM windowed aggregation:\n{}", insertSQL);
            // This defines the data flow but doesn't block or execute the job yet.
            tEnv.executeSql(insertSQL);
            LOG.info("SQL query submitted for execution plan generation.");

            // Do NOT call env.execute() here! Table API SQL jobs are managed by executeSql.

        } catch (Exception e) {
            LOG.error("An error occurred during Flink job execution:", e);
            // Re-throw or handle appropriately
            throw e;
        }
    }
}