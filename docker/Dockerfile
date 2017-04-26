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
      oracle-java8-installer

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
WORKDIR /home/$username

#
# clean up...
#
RUN apt-get clean autoclean && \
    apt-get autoremove -y && \
        rm -rf /var/lib/{apt,dpkg,cache,log}/

#
# Switch to the user...
#
USER $username

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
CMD bin/serene-start
