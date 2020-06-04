from confluent_kafka import Producer
from google.cloud import storage

import json
import os
import sys

release_date = os.environ["RELEASE_DATE"]
bucket_name = os.environ["GCS_BUCKET"]
ingest_summary = json.loads(os.environ["INGEST_SUMMARY"])

kafka_topic = os.environ["KAFKA_TOPIC"]
kafka_url = os.environ["KAFKA_URL"]
kafka_user = os.environ["KAFKA_USERNAME"]
kafka_password = os.environ["KAFKA_PASSWORD"]

client = storage.Client()
bucket = client.get_bucket(bucket_name)

def get_data_files(summary):
  files = []
  for event in ['created', 'updated', 'deleted']:
    count = summary[f'{event}-count']
    if int(count) > 0:
      gs_path = summary[f'{event}-prefix']
      blobs = bucket.list_blobs(prefix=gs_path)
      files.extend([blob.name for blob in blobs])

  return files

all_files = []
for table_summary in ingest_summary:
  all_files.extend(get_data_files(table_summary))

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
