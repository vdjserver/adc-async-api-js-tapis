# Base Image
FROM ubuntu:22.04

MAINTAINER VDJServer <vdjserver@utsouthwestern.edu>

# Install OS Dependencies
RUN export DEBIAN_FRONTEND=noninteractive && apt-get update && apt-get install -y --fix-missing \
    make \
    gcc g++ \
    supervisor \
    wget \
    xz-utils \
    git

##################
##################

# node
ENV NODE_VER v18.17.1
RUN wget https://nodejs.org/dist/$NODE_VER/node-$NODE_VER-linux-x64.tar.xz
RUN tar xf node-$NODE_VER-linux-x64.tar.xz
RUN cp -rf /node-$NODE_VER-linux-x64/bin/* /usr/bin
RUN cp -rf /node-$NODE_VER-linux-x64/lib/* /usr/lib
RUN cp -rf /node-$NODE_VER-linux-x64/include/* /usr/include
RUN cp -rf /node-$NODE_VER-linux-x64/share/* /usr/share

##################
##################

# setup vdj user
RUN echo "vdj:x:816290:803419:VDJServer,,,:/home/vdj:/bin/bash" >> /etc/passwd
RUN echo "G-803419:x:803419:vdj" >> /etc/group
RUN mkdir /home/vdj
RUN chown vdj /home/vdj
RUN chgrp G-803419 /home/vdj

# Setup supervisor
COPY docker/supervisor/supervisor.conf /etc/supervisor/conf.d/

##################
##################

# Copy project source
RUN mkdir /adc-async-api-js-tapis
COPY . /adc-async-api-js-tapis

# build vdjserver-schema and airr-js from source
RUN cd /adc-async-api-js-tapis/vdjserver-schema/airr-standards/lang/js && npm install --unsafe-perm
RUN cd /adc-async-api-js-tapis/vdjserver-schema && npm install --unsafe-perm

RUN cd /adc-async-api-js-tapis && npm install

# ESLint
RUN cd /adc-async-api-js-tapis && npm run eslint app
RUN cd /adc-async-api-js-tapis && npm run eslint vdj-tapis-js

# Copy AIRR spec
RUN cp /adc-async-api-js-tapis/vdjserver-schema/airr-standards/specs/adc-api-async.yaml /adc-async-api-js-tapis/app/api/swagger/adc-api-async.yaml

CMD ["bash", "/adc-async-api-js-tapis/docker/scripts/start-service.sh"]
