import json
import google.auth
from google.cloud import storage, pubsub
import google.cloud.exceptions


PROJECT = "equancyrandd"
DL_BUCKET = "eqlab-datalake-landing"
storage_client = storage.Client()
bucket = storage_client.get_bucket(DL_BUCKET)


def main():
    creds, project_id = google.auth.default()
    
    sync_events()
    async_events()


def sync_events():
    with pubsub.SubscriberClient() as subscriber:
        subscription_path = subscriber.subscription_path(
            PROJECT, "datalake-landing-events-sub"
        )
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 1}
        )
        ack = []
        for msg in response.received_messages:
            event = json.loads(msg.message.data.decode("utf-8"))
            print(f"Pulled {event['bucket']}/{event['name']}")
            ack.append(msg.ack_id)
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack}
        )


def event_handler(message):
    event = json.loads(message.data.decode("utf-8"))
    print(f"Received {event['bucket']}/{event['name']}")
    message.ack()


def async_events():
    with pubsub.SubscriberClient() as subscriber:
        subscription_path = subscriber.subscription_path(
            PROJECT, "datalake-landing-events-sub"
        )
        future = subscriber.subscribe(subscription_path, event_handler)
        try:
            future.result()
        finally:
            future.cancel()


if __name__ == "__main__":
    main()
