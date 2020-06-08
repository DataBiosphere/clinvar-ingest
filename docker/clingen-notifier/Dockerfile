FROM python:3-slim
ARG GCS_CLIENT_VERSION
ARG KAFKA_CLIENT_VERSION
RUN pip install google-cloud-storage==${GCS_CLIENT_VERSION} confluent-kafka==${KAFKA_CLIENT_VERSION}
