FROM ubuntu:14.04

MAINTAINER Jeltje van Baren, jeltje.van.baren@gmail.com

#RUN apt-get update && apt-get install -y \
#        g++ \
#        make


# copy libmaus and bamsort
#WORKDIR /home

# exec is the local directory
#ADD exec/include/* /usr/local/include/ 
ADD exec/lib/* /usr/local/lib/
ADD exec/bamsort /usr/local/bin/

# running ldconfig after make install so that the newly installed libraries are found.
RUN ldconfig /usr/local/lib

# Setting the LD_LIBRARY_PATH environment variable so the systems dynamic linker can find the newly installed libraries.
#RUN LD_LIBRARY_PATH="/opt/gtk/lib"

# Updating PATH environment program so that utility binaries installed by the various libraries will be found.
#RUN PATH="/opt/gtk/bin:$PATH"

RUN mkdir /data
WORKDIR /data

# clean up
#RUN apt-get clean && rm -rf /var/lib/apt/lists/* /home/biobam /home/libmaus

#ENTRYPOINT ["bash", "/usr/local/bin/bamsort"]
