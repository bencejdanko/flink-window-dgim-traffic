#!/bin/bash

# --- Configuration ---
JAR_NAME="my-flink-dgim-1.0-SNAPSHOT.jar"
# Local path where Maven copies the JAR (relative to where you run the script)
FLINK_USER_CODE_DIR_HOST="./src/usrcode"
# Path inside the Flink JobManager container where the JAR is mounted/available
FLINK_JAR_PATH_CONTAINER="/opt/flink/usrcode/$JAR_NAME"
JOBMANAGER_CONTAINER="jobmanager" # Name of your Flink JobManager Docker container
DEFAULT_PARALLELISM=1

# --- Functions ---

# Function to display usage instructions and exit
usage() {
    echo "Usage: $0 <job_type>"
    echo "  job_type: 'tumbling' or 'sliding'"
    exit 1
}

# Function to clean up (stop the Flink job)
cleanup() {
    echo # Add a newline after ^C
    if [ -n "$JOB_ID" ]; then
        # Use the JOB_NAME captured earlier for a more informative message
        echo "Ctrl+C detected. Stopping Flink job '$JOB_NAME' ($JOB_ID)..."
        # Use docker exec to cancel the job. Suppress output for cleaner exit.
        docker exec "$JOBMANAGER_CONTAINER" flink cancel "$JOB_ID" > /dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "Flink job $JOB_ID cancellation request sent."
        else
            # Job might have already finished or failed
            echo "Warning: Failed to send cancellation request for job '$JOB_NAME'. Job might have already finished." >&2
        fi
    else
        echo "Ctrl+C detected, but no Flink job ID was captured to stop."
    fi
    exit 0 # Exit the script gracefully
}

# --- Main Script Logic ---

# 1. Argument Validation
if [ "$#" -ne 1 ]; then
    echo "Error: Missing job_type argument." >&2
    usage
fi
JOB_TYPE=$1

# 2. Determine Main Class and Job Name based on argument
case "$JOB_TYPE" in
    tumbling)
        MAIN_CLASS="com.bencejdanko.flink.StreamingJob"
        JOB_NAME="Tumbling Window DGIM"
        ;;
    sliding)
        MAIN_CLASS="com.bencejdanko.flink.SlidingWindowStreamingJob"
        JOB_NAME="Sliding Window DGIM"
        ;;
    *)
        echo "Error: Invalid job_type '$JOB_TYPE'." >&2
        usage
        ;;
esac

echo "Selected job type: $JOB_TYPE"
echo "Main class: $MAIN_CLASS"
echo "Job name: $JOB_NAME"

# Trap SIGINT (Ctrl+C) and call the cleanup function
# Needs to be set *after* JOB_NAME is defined for the cleanup message
trap cleanup SIGINT

# 3. Build the project (contains both jobs now)
echo "Building project (JAR: $JAR_NAME)..."
if ! mvn package clean > build.log 2>&1; then
    echo "Build failed. Check build.log for details." >&2
    exit 1
fi

# --- Sanity Check: Verify JAR exists locally after build ---
LOCAL_JAR_PATH="$FLINK_USER_CODE_DIR_HOST/$JAR_NAME"
if [ ! -f "$LOCAL_JAR_PATH" ]; then
    echo "Error: JAR file '$LOCAL_JAR_PATH' not found after build." >&2
    echo "Check Maven build log (build.log) and pom.xml's maven-antrun-plugin configuration." >&2
    # Optional: uncomment below to show build log on error
    # cat build.log
    exit 1
fi
# --- End Sanity Check ---

rm build.log # Remove log file if build was successful
echo "Build successful. JAR found locally at $LOCAL_JAR_PATH."
echo "(Ensure this directory is correctly volume-mapped to $FLINK_JAR_PATH_CONTAINER in Docker)"


# 4. Submit the selected Flink job (Simplified Command Execution)
echo "Starting Flink job '$JOB_NAME' via container '$JOBMANAGER_CONTAINER'..."
echo "Command: docker exec \"$JOBMANAGER_CONTAINER\" flink run -d -c \"$MAIN_CLASS\" \"$FLINK_JAR_PATH_CONTAINER\" --parallelism \"$DEFAULT_PARALLELISM\""

# Execute flink run directly inside the container, capturing combined stdout/stderr
# Quote arguments containing variables that are passed *to* flink run inside the container
JOB_OUTPUT=$(docker exec "$JOBMANAGER_CONTAINER" flink run -d \
    -c "$MAIN_CLASS" \
    "$FLINK_JAR_PATH_CONTAINER" \
    --parallelism "$DEFAULT_PARALLELISM" 2>&1) # Capture stderr too
FLINK_RUN_EXIT_CODE=$?

# 5. Check Submission Result & Extract Job ID
# Check if the docker exec command itself failed OR if flink run returned non-zero
if [ $FLINK_RUN_EXIT_CODE -ne 0 ]; then
    echo "Failed to submit Flink job '$JOB_NAME' (Exit Code: $FLINK_RUN_EXIT_CODE)." >&2
    echo "Output from Flink execution:" >&2
    echo "$JOB_OUTPUT" >&2
    exit 1
fi

# Extract the Job ID from the output
JOB_ID=$(echo "$JOB_OUTPUT" | grep -oE '[0-9a-f]{32}')

# Check if we successfully extracted a Job ID (Flink might submit but fail later)
# Check the output for common submission success messages as well.
if [ -z "$JOB_ID" ] || ! echo "$JOB_OUTPUT" | grep -q -E "Job has been submitted with JobID|job has been submitted with Job ID"; then
    echo "Job '$JOB_NAME' submission command executed, but failed to confirm successful submission or extract Job ID." >&2
    echo "Output from Flink execution:" >&2
    echo "$JOB_OUTPUT" >&2
    echo "Check Flink JobManager logs for more details." >&2
    echo "You may need to stop the job manually using 'docker exec $JOBMANAGER_CONTAINER flink list' and 'flink cancel'." >&2
    exit 1
fi

echo "Flink job '$JOB_NAME' submitted successfully with Job ID: $JOB_ID"
echo "Script is now waiting. Press Ctrl+C to stop the Flink job and exit."

# 6. Wait for Ctrl+C
while true; do
    sleep 3600 # Sleep for a long time, effectively waiting for the trap
done

# This line should not be reached because the trap handler exits the script
echo "Script exiting unexpectedly."
exit 1