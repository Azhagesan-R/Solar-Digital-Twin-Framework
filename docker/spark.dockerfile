FROM apache/spark:3.5.8-java17-python3

WORKDIR /opt/spark/work-dir

COPY data_preprocessing/preprocess.py /opt/spark/work-dir/preprocess.py

USER spark

ENTRYPOINT ["/opt/spark/bin/spark-submit"]

CMD ["preprocess.py"]