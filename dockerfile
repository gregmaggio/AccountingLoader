FROM amd64/ubuntu:latest

RUN apt-get update; apt-get install -y curl; apt-get install -y ftp; apt-get install -y netbase;
RUN curl -fL -o /opt/openjdk-19+36_linux-x64_bin.tar.gz "https://download.java.net/openjdk/jdk19/ri/openjdk-19+36_linux-x64_bin.tar.gz";
RUN gunzip /opt/openjdk-19+36_linux-x64_bin.tar.gz
RUN tar -xvf /opt/openjdk-19+36_linux-x64_bin.tar -C /opt

ENV JAVA_HOME /opt/jdk-19
ENV PATH $JAVA_HOME/bin:$PATH

RUN mkdir /opt/csv
RUN mkdir /opt/avro
RUN mkdir /opt/logs
ADD target/accountingloader-1.0.0-jar-with-dependencies.jar /opt/loader.jar
