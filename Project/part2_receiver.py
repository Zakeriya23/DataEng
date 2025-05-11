import json
import logging
from datetime import datetime, timedelta
from google.cloud import pubsub_v1
import psycopg2

# config
SUBSCRIPTION_PATH = "projects/somalias-data-eng/subscriptions/breadcrumbs-sub"
DB_CONFIG = {
    'dbname':   "",
    'user':     "",
    'password': "",
    'host':     "",
    'port':     
}

# logging
logging.basicConfig(level=logging.INFO,
                    format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger("receiver")

# postgres
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur  = conn.cursor()

# state
previous_records = {}   # for speed & inter-record checks
days_with_trip   = set()  # for summary assertion

def parse_opd_date(opd: str) -> datetime:
    # parse "31DEC2022:00:00:00"
    date_part = opd.split(":", 1)[0].title()
    return datetime.strptime(date_part, "%d%b%Y")


def validate_record(rec: dict) -> bool:
    # 8 Existence Assertions
    for f in (
        "VEHICLE_ID", "EVENT_NO_TRIP", "EVENT_NO_STOP",
        "OPD_DATE", "ACT_TIME", "METERS",
        "GPS_LATITUDE", "GPS_LONGITUDE"
    ):
        if rec.get(f) is None:
            logger.warning("Missing %s", f)
            return False

    # 7 Limit Checks
    act = rec["ACT_TIME"]
    if not (0 <= act <= 86399):
        logger.warning("Bad ACT_TIME: %s", act)
        return False

    lat, lon = rec["GPS_LATITUDE"], rec["GPS_LONGITUDE"]
    if not (45.0 <= lat <= 46.0 and -123.5 <= lon <= -122.0):
        logger.warning("Coords out of bounds: %s,%s", lat, lon)
        return False

    sat = rec.get("GPS_SATELLITES")
    if sat is not None and not (4 <= sat <= 20):
        logger.warning("Bad GPS_SATELLITES: %s", sat)
        return False

    hdop = rec.get("GPS_HDOP")
    if hdop is not None and hdop <= 0:
        logger.warning("Bad GPS_HDOP: %s", hdop)
        return False

    # Intra-record Checks
    if rec["GPS_LATITUDE"] is None or rec["GPS_LONGITUDE"] is None:
        # skip if coords missing
        return False

    if rec["METERS"] == 0 and act > 0:
        logger.warning("Zero METERS at ACT_TIME>0")
        return False

    if hdop is not None and hdop > 10:
        logger.warning("High GPS_HDOP: %s", hdop)
        return False

    # Inter-record Checks
    tid  = rec["EVENT_NO_TRIP"]
    prev = previous_records.get(tid)


    if prev and prev.get("OPD_DATE") != rec["OPD_DATE"]:
        previous_records.pop(tid)
        prev = None
    #if prev:
        #logger.warning("Trip: %s Prev time: %s  vs Current: %s",tid, rec["ACT_TIME"], prev["ACT_TIME"])

        #if rec["ACT_TIME"] < prev["ACT_TIME"]:
        #    logger.warning("Time < : Trip: %s Prev time: %s  < Current: %s",tid, prev["ACT_TIME"],rec["ACT_TIME"])
        #    logger.warning("ACT_TIME moved backwards on trip %s", tid)
        #    return False
        #if rec["METERS"] < prev["METERS"]:
        #    logger.warning("METER: Trip: %s Prev Meter: %s  vs Current: %s",tid, prev["METERS"],rec["METERS"])
            #logger.warning("METERS decreased on trip %s", tid)
        #   return False
        #if rec["ACT_TIME"] == prev["ACT_TIME"]:
        ##    logger.warning("DUP: Trip: %s Prev Meter: %s  vs Current: %s",tid, prev["ACT_TIME"],rec["ACT_TIME"])
        #    logger.warning("Duplicate timestamp on trip %s", tid)
        #   return False

    return True


def transform_record(rec: dict) -> dict:
    # timestamp
    base = parse_opd_date(rec["OPD_DATE"])
    rec["tstamp"] = base + timedelta(seconds=rec["ACT_TIME"])

    # speed Statistical assertion
    tid  = rec["EVENT_NO_TRIP"]
    prev = previous_records.get(tid)
    if prev:
        dt = rec["ACT_TIME"] - prev["ACT_TIME"]
        ds = rec["METERS"]   - prev["METERS"]
        rec["speed"] = (ds/dt) if dt > 0 else 0.0
    else:
        rec["speed"] = 0.0

    return rec


def callback(message):
    # parse & archive raw
    try:
        rec = json.loads(message.data.decode())
    except Exception as e:
        logger.error("JSON decode failed: %s", e)
        message.ack()
        return

    fn = datetime.now().strftime("%Y-%m-%d") + ".json"
    try:
        with open(fn, "a") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception as e:
        logger.error("Archive write failed: %s", e)

    # validation
    if not validate_record(rec):
        message.ack()
        return

    # transform
    try:
        rec = transform_record(rec)
    except Exception as e:
        logger.error("Transform error: %s", e)
        message.ack()
        return

    # speed sanity (Statistical)
    if rec["speed"] > 35.0:
        logger.warning("Speed out of range: %.2f m/s", rec["speed"])
        message.ack()
        return
    # Non-negative speed assertion
    if rec["speed"] < 0:
        logger.warning("Negative speed on trip %s: %.2f m/s",
        rec["EVENT_NO_TRIP"], rec["speed"])
        message.ack()
        return

    # update state for next record
    previous_records[rec["EVENT_NO_TRIP"]] = {
        "ACT_TIME": rec["ACT_TIME"],
        "METERS":   rec["METERS"],
        "OPD_DATE": rec["OPD_DATE"]
    }

    # Referential Integrity: ensure Trip exists
    try:
        cur.execute(
            """
            INSERT INTO Trip (trip_id, vehicle_id)
            VALUES (%s, %s)
            ON CONFLICT (trip_id) DO NOTHING
            """,
            (rec["EVENT_NO_TRIP"], rec["VEHICLE_ID"])
        )
    except Exception as e:
        logger.error("Trip upsert failed: %s", e)

    # insert BreadCrumb
    try:
        cur.execute(
            """
            INSERT INTO BreadCrumb
              (tstamp, latitude, longitude, speed, trip_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                rec["tstamp"],
                rec["GPS_LATITUDE"],
                rec["GPS_LONGITUDE"],
                rec["speed"],
                rec["EVENT_NO_TRIP"],
            )
        )
    except Exception as e:
        logger.error("BreadCrumb insert failed: %s", e)

    # Summary Assertions: track days with at least one trip
    days_with_trip.add(rec["tstamp"].date())

    message.ack()


if __name__ == "__main__":
    logger.info("Starting receiver on %s", SUBSCRIPTION_PATH)
    subscriber = pubsub_v1.SubscriberClient()
    future    = subscriber.subscribe(SUBSCRIPTION_PATH, callback=callback)
    try:
        future.result()
    except KeyboardInterrupt:
        logger.info("Shutdown requested")
    finally:
        future.cancel()
        conn.close()
        # Summary: ensure each day had at least one trip
        if not days_with_trip:
            logger.error("No trips processed today!")
        else:
            logger.info("Trips processed for days: %s", sorted(days_with_trip))
        logger.info("Receiver stopped cleanly.")
