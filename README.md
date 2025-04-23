Network traffic monitor where we count bits flowing through a network interface in various time windows.

Set env variables beforehand
=====================================

```bash
export UID=$(id -u)
export GID=$(id -g)
```

These will ensure in development, you can freely edit `./src/pipeline` files/jobs.

Start the dockerized enviroment
=====================================

```bash
docker-compose up -d --build
```

Connect to the WebUI
===================================

http://localhost:8082/#/overview

Start the producer
======================================

```bash
# ensure you have created a venv and installed requirements.txt
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python3 generate-traffic.py
```

Check produced messages
=====================================

You can test check if the messages have been produced with:

```bash
docker exec -it flink-window-dgim-traffic_kafka_1 kafka-console-consumer.sh --bootstrap-server flink-window-dgim-traffic_kafka_1:9093 --topic network_traffic --from-beginning
```

```bash
docker exec -it flink-window-dgim-traffic_kafka_1 kafka-console-consumer.sh --bootstrap-server flink-window-dgim-traffic_kafka_1:9093 --topic tumble_window_output
```

```bash
docker exec -it flink-window-dgim-traffic_kafka_1 kafka-console-consumer.sh --bootstrap-server flink-window-dgim-traffic_kafka_1:9093 --topic sliding_window_output
```

Run the jobs
======================================

## Java

Run `./test-job.sh` to automatically build the Java files, place them into `src/usrcode`, and run them as a Flink job.

===

## Python

`src/usrcode` will automatically be mounted to the docker container. Create new jobs/files to upload there. Below is an example of how to run the existing jobs:

```bash
docker exec -it jobmanager flink run --python /opt/flink/usrcode/job.py --parallelism 1
docker exec -it jobmanager flink run --python /opt/flink/usrcode/job2.py --parallelism 1
# add more jobs, etc...
```

Confirm Flink libraries
=================

```bash
docker exec -it jobmanager bash -c "cd /opt/flink/lib && ls"
```