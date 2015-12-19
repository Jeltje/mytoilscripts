#!/usr/bin/env python2.7
#Jeltje van Baren 
"""
Batch script using Toil

Runs adtex on input coverage and baf files.

Dependencies:
Docker  -   apt-get install docker.io
Toil    -   pip install toil
S3AM*   -   pip install --pre S3AM  (optional)
Curl    -   apt-get install curl
"""
import argparse
import base64
from collections import OrderedDict
import hashlib
import os
import subprocess
import multiprocessing
import shutil
import sys
from toil.job import Job


def build_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default=None, help='configuration file with ID and URLs to bam inputs (control, tumor): uuid,url,url,...')
    parser.add_argument('-s', '--ssec', default=None, help='Path to Key File for SSE-C Encryption')
    parser.add_argument('-w', '--white', required=True, help='exome whitelist (bed format)')
    parser.add_argument('-o', '--out', default=None, help='full path where final results will be output')
    parser.add_argument('-3', '--s3_dir', default=None, help='S3 Directory, starting with bucket name. e.g.: '
                                                             'cgl-driver-projects/ckcc/rna-seq-samples/')
    parser.add_argument('-u', '--sudo', dest='sudo', action='store_true', help='Docker usually needs sudo to execute '
                                                                               'locally, but not''when running Mesos '
                                                                               'or when a member of a Docker group.')
    return parser


# Convenience Functions
def generate_unique_key(master_key_path, url):
    """
    Input1: Path to the BD2K Master Key (for S3 Encryption)
    Input2: S3 URL (e.g. https://s3-us-west-2.amazonaws.com/cgl-driver-projects-encrypted/wcdt/exome_bams/DTB-111-N.bam)

    Returns: 32-byte unique key generated for that URL
    """
    with open(master_key_path, 'r') as f:
        master_key = f.read()
    assert len(master_key) == 32, 'Invalid Key! Must be 32 characters. ' \
                                  'Key: {}, Length: {}'.format(master_key, len(master_key))
    new_key = hashlib.sha256(master_key + url).digest()
    assert len(new_key) == 32, 'New key is invalid and is not 32 characters: {}'.format(new_key)
    return new_key

def download_encrypted_file(job, url, key_path):
    """
    Downloads encrypted files from S3 via header injection

    url: str        URL to be downloaded
    key_path: str   Path to the master key needed to derive unique encryption keys per file
    """
    work_dir = job.fileStore.getLocalTempDir()
    file_path = os.path.join(work_dir, os.path.basename(url))

    with open(key_path, 'r') as f:
        key = f.read()
    if len(key) != 32:
        raise RuntimeError('Invalid Key! Must be 32 bytes: {}'.format(key))

    key = generate_unique_key(key_path, url)

    encoded_key = base64.b64encode(key)
    encoded_key_md5 = base64.b64encode(hashlib.md5(key).digest())
    h1 = 'x-amz-server-side-encryption-customer-algorithm:AES256'
    h2 = 'x-amz-server-side-encryption-customer-key:{}'.format(encoded_key)
    h3 = 'x-amz-server-side-encryption-customer-key-md5:{}'.format(encoded_key_md5)
    try:
        subprocess.check_call(['curl', '-fs', '--retry', '5', '-H', h1, '-H', h2, '-H', h3, url, '-o', file_path])
    except OSError:
        raise RuntimeError('Failed to find "curl". Install via "apt-get install curl"')
    assert os.path.exists(file_path)
    return job.fileStore.writeGlobalFile(file_path)


def download_from_url(job, url):
    """
    Downloads a URL that was supplied as an argument to running this script in LocalTempDir.
    After downloading the file, it is stored in the FileStore.

    url: str        URL to be downloaded. filename is derived from URL
    """
    work_dir = job.fileStore.getLocalTempDir()
    file_path = os.path.join(work_dir, os.path.basename(url))
    if not os.path.exists(file_path):
        try:
            subprocess.check_call(['curl', '-fs', '--retry', '5', '--create-dir', url, '-o', file_path])
        except OSError:
            raise RuntimeError('Failed to find "curl". Install via "apt-get install curl"')
    assert os.path.exists(file_path)
    return job.fileStore.writeGlobalFile(file_path)


def return_input_paths(job, work_dir, ids, *args):
    """
    Returns the paths of files from the FileStore

    Input1: Toil job instance
    Input2: Working directory
    Input3: jobstore id dictionary
    Input4: names of files to be returned from the jobstore

    Returns: path(s) to the file(s) requested -- unpack these!
    """
    paths = OrderedDict()
    for name in args:
        if not os.path.exists(os.path.join(work_dir, name)):
            file_path = job.fileStore.readGlobalFile(ids[name], os.path.join(work_dir, name))
        else:
            file_path = os.path.join(work_dir, name)
        paths[name] = file_path
        if len(args) == 1:
            return file_path

    return paths.values()


# Start of Job Functions
######
def download_shared_files(job, input_args):
    """
    Downloads shared files that are used by all samples for alignment and places them in the jobstore.

    input_args: dict        Input arguments (passed from main())
    """
    shared_files = ['white.bed']
    shared_ids = {}
    for fname in shared_files:
        url = input_args[fname]
        shared_ids[fname] = job.addChildJobFn(download_from_url, url).rv()
    job.addFollowOnJobFn(parse_config, shared_ids, input_args)

def parse_config(job, shared_ids, input_args):
    """
    Stores the UUID and urls associated with the input files to be retrieved.
    Configuration file has one sample per line, with the following format:  UUID,1st_url,2nd_url

    shared_ids: dict        Dictionary of fileStore IDs for the shared files downloaded in the previous step
    input_args: dict        Input argumentts
    """
    samples = []
    config = input_args['config']
    with open(config, 'r') as f_in:
        for line in f_in:
            line = line.strip().split(',')
            uuid = line[0]
            # there are three urls: one baf file and two coverage files
            urls = line[2:]
            samples.append((uuid, urls))
    input_args['cpu_count'] = multiprocessing.cpu_count()
    job_vars = (input_args, shared_ids)
    for sample in samples:
        job.addChildJobFn(download_inputs, job_vars, sample, cores=input_args['cpu_count'])
        #job.addChildJobFn(download_inputs, job_vars, sample, cores=input_args['cpu_count'], memory='20 G', disk='100 G')

def download_inputs(job, job_vars, sample):
    """
    Downloads the sample inputs (coverage and baf files)

    job_vars: tuple         Contains the dictionaries: input_args and ids
    sample: tuple           Contains the uuid (str) and urls (list of strings)
    """
    input_args, ids = job_vars
    uuid, baf, urls = sample
    input_args['uuid'] = uuid
    for i, file in enumerate(['sample.baf', 'control.cov', 'tumor.cov']):
        if input_args['ssec']:
            key_path = input_args['ssec']
            ids[file] = job.addChildJobFn(download_encrypted_file, urls[i], key_path).rv()
        else:
            ids[file] = job.addChildJobFn(download_from_url, urls[i]).rv()
    job.addFollowOnJobFn(run_adtex, job_vars, cores=job_vars['cpu_count'])

def run_adtex(job, job_vars):
    """
    This module runs the Adtes variant caller including zygosity output. The output is a directory of files
    which should be tarred

    job_vars: tuple         Contains the dictionaries: input_args and ids
    """
    # Unpack variables
    input_args, ids = job_vars
    work_dir = job.fileStore.getLocalTempDir()
    sudo = input_args['sudo']
    cores = input_args['cpu_count']
    # Retrieve samples
    return_input_paths(job, work_dir, ids, 'sample.baf', 'tumor.cov', 'control.cov')
    # Retrieve input files
    return_input_paths(job, work_dir, ids, 'white.bed')

    # Call: Adtex
    uuid = input_args['uuid']
    #### WORK HERE (must be ready to tar output)
    # FIXME muse_vcf = os.path.join(work_dir, uuid + '.muse.vcf')
    parameters = ['-n', 'control.cov'.format(uuid + '.control.coverage'),
                '-t', 'tumor.cov'.format(uuid + '.tumor.coverage'),
                '-b', 'white.bed',
                '-o', 'adtex_out',
                '-p', '--estimatePloidy', 
                '--baf', 'sample.baf' ]
    docker_call(work_dir=work_dir, tool_parameters=parameters,
                tool='jeltje/adtex', sudo=sudo)
    outtar = os.path.join(work_dir, uuid + 'adtex.tgz')
    make_tarfile(outtar, (os.path.join(work_dir, outdir)))
    # Save in JobStore - I think this is no longer correct, must check
    job.fileStore.updateGlobalFile(ids['tgz'], outtar)


    #ids['muse_vcf'] = job.fileStore.writeGlobalFile(muse_vcf)
    if input_args['s3_dir']:
        job.addChildJobFn(upload_to_s3, job_vars, disk='80G')


def docker_path(file_path):
    """
    Returns the path internal to the docker container (for standard reasons, this is always /data)
    """
    return os.path.join('/data', os.path.basename(file_path))


def docker_call(work_dir, tool_parameters, tool, java_opts=None, outfile=None, sudo=False):
    """
    Makes subprocess call of a command to a docker container.


    tool_parameters: list   An array of the parameters to be passed to the tool
    tool: str               Name of the Docker image to be used (e.g. quay.io/ucsc_cgl/samtools)
    java_opts: str          Optional commands to pass to a java jar execution. (e.g. '-Xmx15G')
    outfile: file           Filehandle that stderr will be passed to
    sudo: bool              If the user wants the docker command executed as sudo
    """
    base_docker_call = 'docker run --rm -v {}:/data'.format(work_dir).split()
    if sudo:
        base_docker_call = ['sudo'] + base_docker_call
    if java_opts:
        base_docker_call = base_docker_call + ['-e', 'JAVA_OPTS={}'.format(java_opts)]
    try:
        if outfile:
            subprocess.check_call(base_docker_call + [tool] + tool_parameters, stdout=outfile)
        else:
            subprocess.check_call(base_docker_call + [tool] + tool_parameters)
    except subprocess.CalledProcessError:
        raise RuntimeError('docker command returned a non-zero exit status. Check error logs.')
    except OSError:
        raise RuntimeError('docker not found on system. Install on all nodes.')

def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def upload_to_s3(job, job_vars):
    """
    Uploads a file to S3 via S3AM 

    job_vars: tuple         Contains the dictionaries: input_args and ids
    """
    # Unpack variables
    input_args, ids = job_vars
    uuid = input_args['uuid']
    key_path = input_args['ssec']
    work_dir = job.fileStore.getLocalTempDir()
    # Parse s3_dir to get bucket and s3 path
    s3_dir = input_args['s3_dir']
    bucket_name = s3_dir.lstrip('/').split('/')[0]
    bucket_dir = '/'.join(s3_dir.lstrip('/').split('/')[1:])
    base_url = 'https://s3-us-west-2.amazonaws.com/'
    # FIXME url = os.path.join(base_url, bucket_name, bucket_dir, uuid + '.muse.vcf')
    # Retrieve file to be uploaded
    # FIXME job.fileStore.readGlobalFile(ids['muse_vcf'], os.path.join(work_dir, uuid + '.muse.vcf'))
    # Upload to S3 via S3AM
    s3am_command = ['s3am',
                    'upload',
    # FIXME                'file://{}'.format(os.path.join(work_dir, uuid + '.muse.vcf')),
                    bucket_name,
    # FIXME                os.path.join(bucket_dir, uuid + '.muse.vcf')]
    subprocess.check_call(s3am_command)



if __name__ == "__main__":
    # Define Parser object and add to toil
    parser = build_parser()
    Job.Runner.addToilOptions(parser)
    args = parser.parse_args()

    # Store input_URLs for downloading
    inputs = {'config': args.config,
              'white.bed': args.white,
              'ssec':args.ssec,
              'output_dir': args.out,
              's3_dir': args.s3_dir,
              'cpu_count': None}

    # Launch jobs
    Job.Runner.startToil(Job.wrapJobFn(download_shared_files, inputs), args)
