from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import time
start_time = time.time()

project_id = "somalias-data-eng"
subscription_id = "my-sub"
timeout = 10.0  

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

count =0
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    global count
    count += 1
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening on {subscription_path}...\n")
with subscriber:
    try:
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
print(f"Total messages received: {count}")
print(f"Producer took {time.time() - start_time:.2f} seconds")