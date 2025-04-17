import json
from datetime import datetime
from google.cloud import pubsub_v1

subscription_path = "projects/somalias-data-eng/subscriptions/breadcrumbs-sub"
subscriber = pubsub_v1.SubscriberClient()

def callback(message):
    # Determine the daily file name
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"{today}.json"
    try:
        # Decode message data
        record = json.loads(message.data.decode("utf-8"))
        record_str = json.dumps(record)
        with open(filename, "a", encoding="utf-8") as f:
            f.write(record_str + "\n")
    except Exception as err:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(f"Error processing message: {err}\n")
    message.ack()
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()