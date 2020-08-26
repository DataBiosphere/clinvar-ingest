from confluent_kafka import Producer
from google.cloud import storage

import json
import os
import sys

release_date = os.getenv("RELEASE_DATE")
bucket_name = os.getenv("GCS_BUCKET")
storage_project = os.getenv("STORAGE_PROJECT")
gcs_prefix = os.getenv("GCS_PREFIX")
kafka_topic = os.getenv("KAFKA_TOPIC")
kafka_url = os.getenv("KAFKA_URL")
kafka_user = os.getenv("KAFKA_USERNAME")
kafka_password = os.getenv("KAFKA_PASSWORD")

client = storage.Client()
client = storage.Client(storage_project)
bucket = client.get_bucket(bucket_name)
all_files = [blob.name for blob in bucket.list_blobs(prefix=gcs_prefix)]

message = {
  'release_date': release_date,
  'bucket': bucket_name,
  'files': all_files
}

kafka_producer = Producer({
  'bootstrap.servers': kafka_url,
  'security.protocol': 'SASL_SSL',
  'sasl.mechanism': 'PLAIN',
  'ssl.endpoint.identification.algorithm': 'https',
  'sasl.username': kafka_user,
  'sasl.password': kafka_password,
  'retry.backoff.ms': 500
})

def delivery_report(err, msg):
  if err is not None:
    sys.stderr.write(f'Message delivery failed: {err}\n')
    raise Exception
  else:
    sys.stderr.write(f'Message delivered to {msg.topic()} [{msg.partition()}]\n')

kafka_producer.produce(kafka_topic, json.dumps(message).encode('utf-8'), callback=delivery_report)
kafka_producer.flush()
