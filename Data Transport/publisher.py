from google.cloud import pubsub_v1

project_id = "somalias-data-eng"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

for n in range(1, 10):
    data_str = f"Message number {n}"
    data = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(f"Published: {data_str} -> {future.result()}")
print(f"Published messages to {topic_path}.")
