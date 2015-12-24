#!/usr/bin/env bash
# Jeltje van Baren
#
# This script provides all configuration necessary to run the Toil pipeline on a toil cluster.
# It assumes there is a local file: zygosity_config.csv.  One sample per line: uuid,baf,url_normal,url_tumor.
#
# If --ssec is used, the program assumes input files are encrypted in S3 when retrieving them.
# If --sudo flag is used, 'sudo' will be prepended to the Docker subprocess call
# If --s3_dir is used, the final VCF will be uploaded to S3 using S3AM (pip install --pre s3am, need ~/.boto)
#
# Modify TMPDIR parameter to change location of tmp files.
# Modify first argument to change location of the local fileStore
# Uncomment the final line to resume your Toil job in the event of job failure.
python toil_adtex_zygosity.py \
aws:us-west-2:jeltje-zy-adtex-run-1 \
--retryCount 0 \
--config /home/mesosbox/shared/zygosity_config.csv \
--white https://s3-us-west-2.amazonaws.com/varscan-hg19-input/SeqCapTargets.bed 
--ssec '/home/mesosbox/shared/master.key' \
--s3_dir 'cgl-driver-projects/wcdt/variants/' \
--sseKey=/home/mesosbox/shared/master.key \
--batchSystem="mesos" \
--mesosMaster=mesos-master:5050 \
--workDir=/var/lib/toil \
#--restart
