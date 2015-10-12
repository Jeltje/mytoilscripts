#!/usr/bin/env bash
python toil_varscan.py \
aws:us-west-2:jeltje-toil-varscan-run-21 \
--retryCount 0 \
-c /home/mesosbox/shared/onefig.txt \
--ref https://s3-us-west-2.amazonaws.com/varscan-hg19-input/GRCh37-lite.fa \
--fai https://s3-us-west-2.amazonaws.com/varscan-hg19-input/GRCh37-lite.fa.fai \
--cent https://s3-us-west-2.amazonaws.com/varscan-hg19-input/centNoChr.bed \
--white https://s3-us-west-2.amazonaws.com/varscan-hg19-input/noChrTargets.bed \
-o /var/lib/toil/ \
--s3_dir "jeltje-stuff/outputbams/" \
--batchSystem="mesos" \
--masterIP=mesos-master:5050 \
--workDir=/var/lib/toil

