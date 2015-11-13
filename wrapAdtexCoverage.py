#!/usr/bin/env bash
python toil_adtex.py \
aws:us-west-2:jeltje-toil-adtex-ran-1 \
--retryCount 0 \
-c /home/mesosbox/shared/covfig.txt \
--white https://s3-us-west-2.amazonaws.com/varscan-hg19-input/SeqCapTargets.bed \
--ssec /home/mesosbox/shared/master.key \
-o /var/lib/toil/ \
--s3_dir "cgl-driver-projects-encrypted/wcdt/adtex_out/" \
--sseKey=/home/mesosbox/shared/master.key \
--batchSystem="mesos" \
--masterIP=mesos-master:5050 \
--workDir=/var/lib/toil

