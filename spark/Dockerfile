# Image dùng chung cho spark-master, spark-worker và jupyter
# -> đảm bảo driver và executor cùng phiên bản Python/Spark
FROM apache/spark:3.5.1-python3

USER root

RUN pip install --no-cache-dir \
        jupyterlab==4.1.8 \
        pandas==2.0.3 \
        numpy==1.24.4 \
        pyarrow==15.0.2 \
        matplotlib==3.7.5 \
        seaborn==0.13.2

ENV SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/spark/sbin:$PATH \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/project/spark

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

WORKDIR /opt/project
