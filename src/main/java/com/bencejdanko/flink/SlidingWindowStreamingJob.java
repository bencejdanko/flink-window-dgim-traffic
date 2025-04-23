package com.bencejdanko.flink; // Ensure this matches your directory structure and pom.xml groupId

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// Import SLF4J logging classes
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlidingWindowStreamingJob {

    // Initialize the Logger for this class
    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowStreamingJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Flink DGIM Sliding Window Job Setup...");

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
            // --- NEW Sliding Window Configuration ---
            final String slidingSinkTopic = "sliding_window_output"; // New sink topic
            final String consumerGroupId = "flink-dgim-sliding-group-java"; // Different group ID recommended
            final long slidingWindowSeconds = 60L;  // 60-second window duration (size)
            final long slidingWindowSlideSeconds = 10L; // 10-second slide interval
            // --- End NEW Configuration ---

            // 2. Define Kafka Source Table DDL (Same as before)
            final String sourceDDL = String.format(
                "CREATE TABLE kafka_source (" +
                "  `value` STRING,       " +
                "  `timestamp` STRING,   " +
                "  proctime AS PROCTIME()" + // Add processing time for windowing
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
            // Changed to use 'upsert-kafka' connector with primary key (window_start, window_end)
            // Added window_start column.
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
            // Pass the sliding window *duration* to the DGIM function's constructor
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
                // Keep the timestamp conversion logic for the UDAF's input
                "    TRY_CAST(UNIX_TIMESTAMP(CAST(`timestamp` AS STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS') AS BIGINT)" +
                "  ) AS count_estimate " +
                "FROM TABLE(" +
                // Use HOP for sliding windows: HOP(TABLE table, DESCRIPTOR(timecol), slide_interval, size_interval)
                "  HOP(TABLE kafka_source, DESCRIPTOR(proctime), INTERVAL '%d' SECOND, INTERVAL '%d' SECOND)" +
                ") " +
                // Keep the filter condition
                "WHERE `value` IS NOT NULL AND TRY_CAST(UNIX_TIMESTAMP(CAST(`timestamp` AS STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS') AS BIGINT) IS NOT NULL " +
                "GROUP BY window_start, window_end", // Group by both window bounds
                slidingWindowSlideSeconds, // Interval for the slide
                slidingWindowSeconds       // Interval for the window size (duration)
            );

            LOG.info("Submitting INSERT INTO statement with DGIM sliding window aggregation:\n{}", insertSQL);
            // This defines the data flow but doesn't block or execute the job yet.
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
// </SlidingWindowStreamingJob.java>