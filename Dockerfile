FROM python:3.9.17-slim-buster

ENV LOG_LEVEL=INFO

# Set the working directory to /app/pywis-pubsub
WORKDIR /app/pywis-pubsub

# first install requirements to speed up rebuilds
COPY requirements.txt /app/pywis-pubsub
RUN pip3 install -r requirements.txt

# Copy the current directory contents into the container at /app/pywis-pubsub
COPY . /app/pywis-pubsub

# install pywis-pubsub
RUN python3 setup.py install

# run pywis-pubsub subscribe
CMD pywis-pubsub subscribe --config /tmp/local.yml --verbosity $LOG_LEVEL
