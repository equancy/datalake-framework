import json
import google.auth
from google.cloud import storage, pubsub
import google.cloud.exceptions
from datalake.provider.gcp import StorageNotifications
from datalake.interface import IStorageEvent


PROJECT = "equancyrandd"
DL_BUCKET = "eqlab-datalake-landing"
storage_client = storage.Client()
bucket = storage_client.get_bucket(DL_BUCKET)


class PrintProcessor(IStorageEvent):
    def process(self, storage, path):
        print(f"{storage}/{path}")


def main():
    # creds, project_id = google.auth.default()
    sn = StorageNotifications("datalake-landing-events-sub", PrintProcessor())
    sn.daemon()
    # sync_events()
    # async_events()


# def sync_events():
#     with pubsub.SubscriberClient() as subscriber:
#         subscription_path = subscriber.subscription_path(
#             PROJECT, "datalake-landing-events-sub"
#         )
#         response = subscriber.pull(
#             request={"subscription": subscription_path, "max_messages": 1}
#         )
#         ack = []
#         for msg in response.received_messages:
#             event = json.loads(msg.message.data.decode("utf-8"))
#             print(f"Pulled {event['bucket']}/{event['name']}")
#             ack.append(msg.ack_id)
#         subscriber.acknowledge(
#             request={"subscription": subscription_path, "ack_ids": ack}
#         )


# def event_handler(message):
#     event = json.loads(message.data.decode("utf-8"))
#     print(f"Received {event['bucket']}/{event['name']}")
#     print(event)
#     message.ack()


# def async_events():
#     with pubsub.SubscriberClient() as subscriber:
#         subscription_path = subscriber.subscription_path(
#             PROJECT, "datalake-landing-events-sub"
#         )
#         future = subscriber.subscribe(subscription_path, event_handler)
#         try:
#             future.result()
#         finally:
#             future.cancel()


if __name__ == "__main__":
    main()

# {
#   "kind": "storage#object",
#   "id": "eqlab-datalake-landing/input/test//1634639136595680",
#   "selfLink": "https://www.googleapis.com/storage/v1/b/eqlab-datalake-landing/o/input%2Ftest%2F",
#   "name": "input/test/",
#   "bucket": "eqlab-datalake-landing",
#   "generation": "1634639136595680",
#   "metageneration": "1",
#   "contentType": "text/plain",
#   "timeCreated": "2021-10-19T10:25:36.634Z",
#   "updated": "2021-10-19T10:25:36.634Z",
#   "storageClass": "STANDARD",
#   "timeStorageClassUpdated": "2021-10-19T10:25:36.634Z",
#   "size": "0",
#   "md5Hash": "1B2M2Y8AsgTpgAmY7PhCfg==",
#   "mediaLink": "https://www.googleapis.com/download/storage/v1/b/eqlab-datalake-landing/o/input%2Ftest%2F?generation=1634639136595680&alt=media",
#   "crc32c": "AAAAAA==",
#   "etag": "COC9yqih1vMCEAE=",
#   "temporaryHold": false,
#   "eventBasedHold": false
# }
