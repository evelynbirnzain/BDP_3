import subprocess
import sys
import time
import pymongo
import datetime

db_client = pymongo.MongoClient("mongodb://localhost:27017/")
db = db_client["metrics"]
collection = db["tenantstreamapp"]

f = sys.argv[1]
outfile = open(f"data/{f}.csv", "a")
outfile.write("n_sensors,msg_p_s,p_extra,p_missing,p_wrong,avg_ips,avg_ops,avg_batch_duration,runId\n")
outfile.flush()


def write_metrics_agg(t, speed, n_sensors=10, p_extra=0.0, p_missing=0.0, p_wrong=0.0):
    metrics = []
    for doc in collection.find():
        timestamp = datetime.datetime.strptime(doc["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
        if timestamp > t:
            metrics.append(doc)

    runIds = set([m["runId"] for m in metrics])
    print(f"Found {len(metrics)} metrics from {len(runIds)} runs")

    for runId in runIds:
        run_metrics = [m for m in metrics if m["runId"] == runId]
        avg_ips = sum([m["inputRowsPerSecond"] for m in run_metrics]) / len(run_metrics)
        avg_ops = sum([m["processedRowsPerSecond"] for m in run_metrics]) / len(run_metrics)
        avg_batch_duration = sum([m["batchDuration"] for m in run_metrics]) / len(run_metrics)
        outfile.write(f"{n_sensors},{speed},{p_extra},{p_missing},{p_wrong},{avg_ips},{avg_ops},{avg_batch_duration},{runId}\n")
        outfile.flush()


def wait_for_idle(t):
    while True:
        m = collection.find_one({"state": "idle"}, sort=[("timestamp", pymongo.DESCENDING)])
        print(m)
        if m and datetime.datetime.strptime(m["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ") > t:
            break
        time.sleep(5)


def run_producer(speed, n_sensors=10, p_extra=0.0, p_missing=0.0, p_wrong=0.0):
    proc = subprocess.Popen(
        ["venv\\Scripts\\python.exe", "code\\test-environment\\producer.py", "--n_sensors", str(n_sensors), "--delay",
         str(1/speed), "--p_extra", str(p_extra), "--p_missing", str(p_missing), "--p_wrong", str(p_wrong)])
    time.sleep(60 * 5)
    proc.kill()


def run_test(speed, n_sensors=10, p_extra=0.0, p_missing=0.0, p_wrong=0.0):
    try:
        print(
            f"Testing {speed} messages per second with {n_sensors} sensors and p_extra={p_extra}, p_missing={p_missing}, p_wrong={p_wrong}")
        t = datetime.datetime.utcnow()
        run_producer(speed, n_sensors, p_extra, p_missing, p_wrong)
        write_metrics_agg(t, speed, n_sensors, p_extra, p_missing, p_wrong)
        wait_for_idle(t)
    except Exception as e:
        print(f"Error: {e}")
        outfile.write(f"{n_sensors},{speed},{p_extra},{p_missing},{p_wrong},0,0\n")
        outfile.flush()


# test 1: vary the speed of the producer
def test1_vary_speed():
    for speed in [1, 10, 100, 250, 500, 750, 1000, 1250]:
        run_test(speed)


def test2_vary_wrong_data():
    for p_wrong in [0, 0.25, 0.5, 0.75, 1]:
        run_test(100, p_wrong=p_wrong)
        run_test(100, p_extra=p_wrong)
        run_test(100, p_missing=p_wrong)

def test3_vary_parallelism():
    for n_sensor in [1, 10, 100, 1000]:
        run_test(100, n_sensors=n_sensor)


if __name__ == '__main__':
    # list databases
    print(db_client.list_database_names())

    #test1_vary_speed()
    #test2_vary_wrong_data()
    test3_vary_parallelism()
