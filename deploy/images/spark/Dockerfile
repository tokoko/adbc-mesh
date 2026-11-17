FROM apache/spark:4.1.1-python3
COPY jars/ /opt/spark/jars/
COPY libadbc_driver_relay.so spark_relay_test.py /opt/relay/
# ADBC's JNI library needs GLIBCXX_3.4.31, and this Ubuntu 22.04 base stops at 3.4.30.
COPY libstdc++.so.6 /opt/relay/lib/
ENV LD_LIBRARY_PATH=/opt/relay/lib
