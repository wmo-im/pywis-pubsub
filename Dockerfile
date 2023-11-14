FROM python:3.9.17-slim-buster

ENV LOG_LEVEL=INFO
ENV BROKER_URL=wss://everyone:everyone@globalbroker.meteo.fr:443

ENV VERIFY_DATA=False
ENV VERIFY_MESSAGE=False
ENV VERIFY_CERTS=False

ENV SUBSCRIPTION_POLLING_ENABLED=True
ENV SUBSCRIPTION_POLLING_INTERVAL=10
ENV SUBSCRIPTION_POLLING_URL=http://localhost:5000/wis2/subscriptions/list

# Set the working directory to /app/pywis-pubsub
WORKDIR /app/pywis-pubsub

# first install requirements to speed up rebuilds and install envsubst
COPY requirements.txt /app/pywis-pubsub
RUN apt-get update && apt-get install -y gettext-base && pip3 install -r requirements.txt 

# Copy the current directory contents into the container at /app/pywis-pubsub
COPY . /app/pywis-pubsub

# install pywis-pubsub
RUN python3 setup.py install

# create /tmp/local.yml by replacing environment variables in /app/pywis-pubsub/subscribe-template.yml
RUN envsubst < /app/pywis-pubsub/subscribe-template.yml > /tmp/local.yml

# run pywis-pubsub subscribe
CMD pywis-pubsub subscribe --config /tmp/local.yml --verbosity $LOG_LEVEL
