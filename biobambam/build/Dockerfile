FROM ubuntu:14.04

MAINTAINER Jeltje van Baren, jeltje.van.baren@gmail.com

RUN apt-get update && apt-get install -y \
	g++ \
	make \
	zlib1g-dev \
	wget \
	pkg-config



# Create a new source directory
WORKDIR /home
RUN mkdir biobam-src
WORKDIR /home/biobam-src

# get libmaus (prerequisite for biobambam)
RUN wget https://github.com/gt1/libmaus2/archive/2.0.76-release-20150917153152.tar.gz
RUN tar -zxvf 2.0.76-release-20150917153152.tar.gz
WORKDIR /home/biobam-src/libmaus2-2.0.76-release-20150917153152
RUN ./configure && make -j 8 && make install
WORKDIR /home/biobam-src

# get biobambam
RUN wget https://github.com/gt1/biobambam2/archive/2.0.18-release-20150914145735.tar.gz
RUN tar -zxvf 2.0.18-release-20150914145735.tar.gz
WORKDIR /home/biobam-src/biobambam2-2.0.18-release-20150914145735
RUN ./configure && make -j 8 && make install

RUN ldconfig /usr/local/lib

RUN mkdir /data
WORKDIR /data

# And clean up
RUN apt-get clean && rm -rf /var/lib/apt/lists/* /home/biobam-src

