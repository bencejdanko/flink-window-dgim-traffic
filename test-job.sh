echo "Building..."
mvn package clean > /dev/null || echo "build error" >&2
echo "Starting job..."
docker exec -it jobmanager flink run /opt/flink/usrcode/my-flink-dgim-1.0-SNAPSHOT.jar --parallelism 1