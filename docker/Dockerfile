#
# Docker file for the Serene API
#

FROM ubuntu
MAINTAINER Data61 CSIRO

#
# Install software...
#
RUN \
    apt-get update && \
    apt-get install -y \
      git \
      apt-transport-https \
      software-properties-common
#
# Install Java 1.8...
#
RUN \
    echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
    add-apt-repository -y ppa:webupd8team/java && \
    apt-get update && \
    apt-get install -y \
      oracle-java8-installer \
      openssh-server

#
# Set the java path...
#
ENV JAVA_HOME /usr/lib/jvm/java-8-oracle

#
# Install sbt
#
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
RUN apt-get update
RUN apt-get install -y sbt

#
# Next we create a serene user...
#
ENV username serene
RUN useradd -ms /bin/bash $username && echo "$username:$username" | chpasswd && adduser $username sudo
RUN mkdir -p /home/$username && chown -R $username:$username /home/$username

#
# clean up...
#
RUN apt-get clean autoclean && \
    apt-get autoremove -y && \
        rm -rf /var/lib/{apt,dpkg,cache,log}/

# Add hadoop group
RUN addgroup hadoop && adduser --ingroup hadoop $username

#
# Switch to the user...
#
USER $username
WORKDIR /home/$username

# Setup ssh
RUN ssh-keygen -t rsa -P ''
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN ssh localhost

# Copy hadoop and flink
COPY flink-hadoop.tar.gz /home/$username
RUN tar vxzf flink-hadoop.tar.gz -C /usr/local
RUN cd /usr/local && ln -s ./hadoop-2.7.3 hadoop && chown -R $username:hadoop hadoop

# Hadoop and flink environment variables
ENV HADOOP_HOME /usr/local/hadoop-2.7.3
ENV HADOOP_PREFIX /usr/local/hadoop
ENV HADOOP_COMMON_HOME /usr/local/hadoop
ENV HADOOP_HDFS_HOME /usr/local/hadoop
ENV HADOOP_MAPRED_HOME /usr/local/hadoop
ENV HADOOP_YARN_HOME /usr/local/hadoop
ENV HADOOP_CONF_DIR /usr/local/hadoop/etc/hadoop
ENV YARN_CONF_DIR $HADOOP_PREFIX/etc/hadoop
ENV YARN_HOME $HADOOP_HOME
ENV FLINK_HOME /usr/local/flink-1.1.2
ENV PATH $PATH:$HADOOP_HOME/bin
ENV PATH $PATH:$HADOOP_HOME/sbin

# Format namenode
RUN cd ~ && mkdir -p mydata/hdfs/namenode && mkdir -p mydata/hdfs/datanode
RUN hdfs namenode -format


#
# Copy the git repository
#
RUN \
    mkdir -p bin && \
    mkdir -p jars
COPY jars/* jars/
COPY serene-start bin/

#
# Expose the default port
#
EXPOSE 8080

#
# Launch the server...
#
CMD start-dfs.sh && \
  start-yarn.sh && \
  $FLINK_HOME/bin/start-local.sh && \
  bin/serene-start
