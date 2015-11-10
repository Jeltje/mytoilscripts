#!/usr/bin/env bash
python toil_coverage.py \
aws:us-west-2:jeltje-toil-cov-run-1 \
--retryCount 2 \
-c /home/mesosbox/shared/config.txt \
--white https://s3-us-west-2.amazonaws.com/varscan-hg19-input/SeqCapTargets.bed \
--ssec /home/mesosbox/shared/master.key \
-o /var/lib/toil/ \
--s3_dir "cgl-driver-projects-encrypted/wcdt/adtex_out/" \
--sseKey=/home/mesosbox/shared/master.key \
--batchSystem="mesos" \
--masterIP=mesos-master:5050 \
--workDir=/var/lib/toil

