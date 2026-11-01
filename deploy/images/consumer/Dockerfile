FROM python:3.12-slim
RUN pip install --no-cache-dir pyarrow==23.0.1 adbc-driver-manager==1.11.0
COPY libadbc_driver_relay.so /opt/relay/
COPY consumer_test.py /test/
