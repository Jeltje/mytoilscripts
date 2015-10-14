#!/usr/bin/env bash
python toil_encrypted_varscan.py \
aws:us-west-2:jeltje-toil-evarscan-run-2 \
--retryCount 0 \
-c /home/mesosbox/shared/config.txt \
--ref https://s3-us-west-2.amazonaws.com/cgl-alignment-inputs/genome.fa \
--fai https://s3-us-west-2.amazonaws.com/cgl-alignment-inputs/genome.fa.fai \
--cent https://s3-us-west-2.amazonaws.com/varscan-hg19-input/centromeres.bed \
--white https://s3-us-west-2.amazonaws.com/varscan-hg19-input/SeqCapTargets.bed \
--ssec /home/mesosbox/shared/master.key \
-o /var/lib/toil/ \
--s3_dir "cgl-driver-projects-encrypted/wcdt/varscan_out/" \
--sseKey=/home/mesosbox/shared/master.key \
--batchSystem="mesos" \
--masterIP=mesos-master:5050 \
--workDir=/var/lib/toil

