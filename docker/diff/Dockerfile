FROM python:3-slim

# Create app directory
WORKDIR /app

# Install app dependencies
COPY requirements.txt ./

RUN pip install -r requirements.txt

# Bundle app source
COPY diff.py /app

ENTRYPOINT ["python", "-m", "diff"]