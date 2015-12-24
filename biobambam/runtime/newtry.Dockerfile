FROM ubuntu:14.04

MAINTAINER Jeltje van Baren, jeltje.van.baren@gmail.com

RUN apt-get update && apt-get install -y \
        g++ \
        make


# copy libmaus and bamsort
WORKDIR /home
ADD exec/biobam home/
ADD exec/libmaus home/

WORKDIR /home/libmaus
RUN make install

#WORKDIR /home/biobam
#RUN make install

# Set WORKDIR to /data -- predefined mount location.
RUN mkdir /data
WORKDIR /data

# clean up
#RUN apt-get clean && rm -rf /var/lib/apt/lists/* /home/biobam /home/libmaus

#ENTRYPOINT ["bash", "/usr/local/bin/bamsort"]
