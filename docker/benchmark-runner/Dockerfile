FROM apache/hadoop:3.3.6 AS hadoop

FROM apache/spark:3.5.8-java17

USER root

COPY --from=hadoop /opt/hadoop /opt/hadoop

ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH="${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}"

WORKDIR /workspace

RUN java -version && hdfs version
