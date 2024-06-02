# Deployment/installation guide

## Set up MySimBDP

In `code/.env` set `KAFKA_HOST` to your IP address.

Start the base infrastructure.
```bash
cd code
docker compose up -d zookeeper
docker compose up -d kafka
docker compose up -d spark
docker compose up -d spark-worker
```

Create a topic "measurements" with an appropriate number of partitions.
```bash
docker compose exec kafka kafka-topics.sh --create --topic measurements --partitions 64 --replication-factor 1 --bootstrap-server kafka:9092
```

## Testing

### Setup

Create venv and install requirements.
```bash
python -m venv venv
source venv/bin/activate # Windows: venv\Scripts\activate.bat
pip install -r requirements.txt
```

## Manual testing

Start the producer.
```bash
python .\code\test-environment\producer.py [--n_sensors 10] [--delay 1] [--p_extra 0.1] [--p_missing 0.1] [--p_wrong 0.1]
```

Deploy the Spark processor/tenantstreamapp.
```bash
docker compose up spark-processor [--scale spark-processor=1]
```

## Automated testing
The performance tests I used for the report can be found in `code/test-environment/performance_tests.py`.

```bash
python .\code\test-environment\performance_tests.py
```


