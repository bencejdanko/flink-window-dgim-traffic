from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.udf import udf
import time
import math
import logging # Import logging
import os      # Import os
import traceback # Import traceback

# --- DGIM Class (Keep as is) ---
class DGIM:
    def __init__(self, window_size_seconds):
        self.window_size_seconds = window_size_seconds
        self.buckets = []

    def add(self, bit, timestamp_seconds):
        ts = int(timestamp_seconds)
        if bit == 1:
            self.buckets.insert(0, (ts, 1))
            self._merge_buckets()
        self._expire_buckets(ts)

    def _merge_buckets(self):
        i = 0
        while i < len(self.buckets) - 2:
            if self.buckets[i][1] == self.buckets[i+1][1] == self.buckets[i+2][1]:
                merged_timestamp = self.buckets[i+1][0]
                merged_size = self.buckets[i+1][1] * 2
                self.buckets[i+1] = (merged_timestamp, merged_size)
                del self.buckets[i+2]
            else:
                i += 1

    def _expire_buckets(self, current_time_seconds):
        self.buckets = [b for b in self.buckets if current_time_seconds - b[0] < self.window_size_seconds]

    def estimate(self, current_time_seconds):
        total = 0
        self._expire_buckets(current_time_seconds)
        last_bucket_size = 0
        for i, (ts, size) in enumerate(self.buckets):
            if i == len(self.buckets) - 1:
                last_bucket_size = size // 2
            else:
                total += size
        return total + last_bucket_size

# --- UDF Definition (Keep as is, including MULTISET fix) ---
@udf(result_type=DataTypes.INT(),
     input_types=[DataTypes.MULTISET(DataTypes.STRING()), DataTypes.MULTISET(DataTypes.BIGINT())])
def dgim_estimate_udf(values, timestamps_seconds):
    if not values or not timestamps_seconds:
        # logging.warning("UDF received empty lists/multisets") # Add if needed
        return 0

    window_duration_seconds = 60
    dgim = DGIM(window_duration_seconds)
    paired_data = []
    for v, ts in zip(values, timestamps_seconds):
        if v is None or ts is None:
            continue
        try:
            bit = int(v)
            paired_data.append({'bit': bit, 'timestamp': int(ts)})
        except (ValueError, TypeError):
            # logging.warning(f"Skipping invalid record in UDF: v={v}, ts={ts}") # Add if needed
            continue

    if not paired_data:
        # logging.warning("UDF: No valid data after pairing.") # Add if needed
        return 0

    paired_data.sort(key=lambda x: x['timestamp'])
    latest_timestamp = 0
    for record in paired_data:
        dgim.add(record['bit'], record['timestamp'])
        latest_timestamp = max(latest_timestamp, record['timestamp'])

    if latest_timestamp > 0:
        estimate = dgim.estimate(latest_timestamp)
        # logging.info(f"UDF Estimate for window ending around {latest_timestamp}: {estimate}") # Add if needed
        return estimate
    else:
        # logging.warning("UDF: latest_timestamp is 0, using current time for estimate.") # Add if needed
        return dgim.estimate(math.floor(time.time()))


# --- Main Job Logic ---
def main():
    # Set up logging (like working example)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    try:
        logger.info("Setting up Flink execution environment...")
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_parallelism(1)
        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = StreamTableEnvironment.create(env, environment_settings=settings)
        logger.info("Flink environment created.")

        # --- Kafka Connector JAR Configuration ---
        # Option 1: Explicit Path (use if you are sure it's correct)
        kafka_connector_jar_path = "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar"
        # Option 2: Dynamic Discovery (like working example - more robust)
        # flink_dir = "/opt/flink/lib"
        # kafka_connector_jar_path = None
        # if os.path.exists(flink_dir):
        #     for jar in os.listdir(flink_dir):
        #         if jar.startswith("flink-sql-connector-kafka") and jar.endswith(".jar"):
        #             kafka_connector_jar_path = f"file://{os.path.join(flink_dir, jar)}"
        #             break
        # if not kafka_connector_jar_path:
        #     raise RuntimeError("Could not find Flink Kafka SQL Connector JAR in /opt/flink/lib")

        logger.info(f"Using Kafka Connector JAR: {kafka_connector_jar_path}")
        t_env.get_config().set("pipeline.jars", kafka_connector_jar_path)
        logger.info("Kafka Connector JAR path configured.")

        # --- Define Kafka Source Table ---
        source_topic = "network_traffic"
        kafka_bootstrap_servers = "kafka:9093"
        source_ddl = f"""
        CREATE TABLE kafka_source (
            `value` STRING,
            `timestamp` STRING, -- Keep as STRING to match input JSON
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{source_topic}',
            'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
            'properties.group.id' = 'flink-dgim-group-v2', -- Use a new group id for testing
            'scan.startup.mode' = 'latest-offset', -- Change to latest if earliest causes issues
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """
        logger.info(f"Creating Kafka source table '{source_topic}'...")
        t_env.execute_sql(source_ddl)
        logger.info("Kafka source table created.")

        # --- Register UDF ---
        logger.info("Registering DGIM UDF 'dgim_estimate_sql'...")
        t_env.create_temporary_function("dgim_estimate_sql", dgim_estimate_udf)
        logger.info("DGIM UDF registered.")

        # --- Define Kafka Sink Table ---
        sink_topic = "output-topic"
        sink_ddl = f"""
        CREATE TABLE kafka_sink (
            window_end STRING,
            count_estimate INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{sink_topic}',
            'properties.bootstrap.servers' = '{kafka_bootstrap_servers}',
            'format' = 'json'
            -- Optional: Add Kafka producer properties if needed
            -- 'properties.transaction.timeout.ms' = '900000'
        )
        """
        logger.info(f"Creating Kafka sink table '{sink_topic}'...")
        t_env.execute_sql(sink_ddl)
        logger.info("Kafka sink table created.")

        # --- Define and Execute the Main Processing Query ---
        # Using explicit timestamp format 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'
        # (Adjust SSSSSS based on actual microsecond precision in your data)
        insert_sql = """
        INSERT INTO kafka_sink
        SELECT
            CAST(TUMBLE_END(proctime, INTERVAL '60' SECOND) AS STRING) AS window_end,
            dgim_estimate_sql(
                COLLECT(`value`),
                COLLECT(
                    TRY_CAST(UNIX_TIMESTAMP(CAST(`timestamp` AS STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS') AS BIGINT)
                )
            ) AS count_estimate
        FROM kafka_source
        WHERE TRY_CAST(UNIX_TIMESTAMP(CAST(`timestamp` AS STRING), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS') AS BIGINT) IS NOT NULL
          AND `value` IS NOT NULL
        GROUP BY TUMBLE(proctime, INTERVAL '60' SECOND)
        """
        logger.info("Defining INSERT INTO statement...")
        # This statement defines the operation but doesn't block or fully execute yet
        # The actual job graph execution is triggered by env.execute()
        table_result = t_env.execute_sql(insert_sql)
        logger.info("INSERT INTO statement submitted for execution plan generation.")

        # *** CRUCIAL STEP: Explicitly execute the job graph ***
        # This is the equivalent of the .print() or .execute() in other examples
        # that actually launches the job on the cluster.
        logger.info("Starting Flink job execution...")
        env.execute("Flink DGIM Network Traffic Job (Revised Execution)")
        # The script might appear to hang here in the foreground if run directly,
        # because env.execute() blocks until the job finishes (which it won't for streaming).
        # When run via `flink run`, this correctly submits the job to the cluster to run detached.
        logger.info("Flink job submitted and potentially running in detached mode.")

    except Exception as e:
        logger.error(f"FATAL ERROR during Flink job execution: {e}")
        logger.error(traceback.format_exc()) # Log the full traceback

if __name__ == "__main__":
    main()