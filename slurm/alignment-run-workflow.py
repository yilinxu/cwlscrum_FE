#!/usr/bin/env python
import postgres.utils
import postgres.status
import postgres.mixins
import utils.pipeline
import utils.s3
import tempfile
import datetime
import socket
import time
import argparse
import logging
import uuid
import os
import sys


def get_args():

    parser = argparse.ArgumentParser('TOPMED-alignment-cwl-workflow')

    # Sub parser
    sp = parser.add_subparsers(help='Choose the process you want to run', dest='choice')

    # Build slurm scripts
    p_slurm = sp.add_parser('slurm', help='Options for building slurm scripts.')
    p_slurm.add_argument('--input_table_name', required=True)
    p_slurm.add_argument('--slurm_script_path', required=True)
    p_slurm.add_argument('--resource_core_count', type=int, required=True)
    p_slurm.add_argument('--resource_disk_gb', type=int, required=True)
    p_slurm.add_argument('--resource_memory_gb', type=int, required=True)
    p_slurm.add_argument('--slurm_template_path', required=True)
    p_slurm.add_argument('--pg_user', required=True)
    p_slurm.add_argument('--pg_pw', required=True)
    p_slurm.add_argument('--ref_table_name', required=True)
    p_slurm.add_argument('--cwl_version', required=True)
    p_slurm.add_argument('--docker_version', required=True)
    p_slurm.add_argument('--s3dir', required=True)

    # Build json files and run cwl
    p_input = sp.add_parser('run_cwl', help='Options for building input json and run cwl.')
    p_input.add_argument('--output_basename', required=True)
    p_input.add_argument('--input_id', required=True)
    p_input.add_argument('--output_uuid', required=True)
    p_input.add_argument('--project', required=True)
    p_input.add_argument('--md5', required=True)
    p_input.add_argument('--s3_url', required=True)
    p_input.add_argument('--s3_profile', required=True)
    p_input.add_argument('--s3_endpoint', required=True)
    p_input.add_argument('--thread_count', required=True)
    p_input.add_argument('--pg_user', required=True)
    p_input.add_argument('--pg_pw', required=True)
    p_input.add_argument('--ref_table_name', required=True)
    p_input.add_argument('--cwl_version', required=True)
    p_input.add_argument('--docker_version', required=True)
    p_input.add_argument('--s3dir', required=True)
    p_input.add_argument('--base_dir', required=True)

    return parser.parse_args()


def run_build_slurm_scripts(args):
    # Slurm template and output path
    slurm_script_path = args.slurm_script_path
    slurm_template_path = args.slurm_template_path

    # Postgres configuration
    input_table_name = args.input_table_name

    # Slurm resources and configuration
    resource_core_count = args.resource_core_count
    disk_gigabytes = args.resource_disk_gb
    memory_gigabytes = args.resource_memory_gb
    thread_count = args.thread_count
    pg_user = args.pg_user
    pg_pw = args.pg_pw
    ref_table_name = args.ref_table_name
    cwl_version = args.cwl_version
    docker_version = args.docker_version
    s3_dir = args.s3_dir

    # Get list of bams from input table
    engine = postgres.utils.get_db_engine(args)
    bams = postgres.status.get_bams(engine, input_table_name)

    for bam in bams:
        output_uuid = str(uuid.uuid4())
        input_id = bam.input_id
        project = bam.project
        md5 = bam.md5sum
        s3_url = bam.s3_url
        s3_profile = bam.s3_profile
        s3_endpoint = bam.s3_endpoint
        output_name = os.path.basename(s3_url)
        output_basename = output_name.split(".")[0]

        job_slurm = os.path.join(slurm_script_path, output_uuid + ".sh")
        f_open = open(job_slurm, 'w')
        with open(args.slurm_template_path, 'r') as bam_open:
            for line in bam_open:
                if "XX_THREAD_COUNT_XX" in line:
                    line = line.replace("XX_THREAD_COUNT_XX", str(thread_count))
                elif "XX_MEM_XX" in line:
                    line = line.replace("XX_MEM_XX", str(memory_gigabytes))
                elif "XX_INPUTID_XX" in line:
                    line = line.replace("XX_INPUTID_XX", input_id)
                elif "XX_PROJECT_XX" in line:
                    line = line.replace("XX_PROJECT_XX", project)
                elif "XX_MD5_XX" in line:
                    line = line.replace("XX_MD5_XX", md5)
                elif "XX_S3URL_XX" in line:
                    line = line.replace("XX_S3URL_XX", s3_url)
                elif "XX_S3PROFILE_XX" in line:
                    line = line.replace("XX_S3PROFILE_XX", s3_profile)
                elif "XX_S3ENDPOINT_XX" in line:
                    line = line.replace("XX_S3ENDPOINT_XX", s3_endpoint)
                elif "XX_OUTPUTID_XX" in line:
                    line = line.replace("XX_OUTPUTID_XX", output_uuid)
                elif 'XX_CORE_COUNT_XX' in line:
                    line = line.replace('XX_CORE_COUNT_XX', str(resource_core_count))
                elif 'XX_DISK_GB_XX' in line:
                    line = line.replace('XX_DISK_GB_XX', str(disk_gigabytes))
                elif 'XX_DISK_GB_XX' in line:
                    line = line.replace('XX_DISK_GB_XX', s3dir_bam)
                elif 'XX_OUTPUTBASE_XX' in line:
                    line = line.replace('XX_OUTPUTBASE_XX', output_basename)
                elif 'XX_PG_USER_XX' in line:
                    line = line.replace('XX_PG_USER_XX', pg_user)
                elif 'XX_PG_PW_XX' in line:
                    line = line.replace('XX_PG_PW_XX', pg_pw)
                elif 'XX_REF_TABLE_XX' in line:
                    line = line.replace('XX_REF_TABLE_XX', ref_table_name)
                elif 'XX_CWL_VERSION_XX' in line:
                    line = line.replace('XX_CWL_VERSION_XX', cwl_version)
                elif 'XX_DOCKER_VERSION_XX' in line:
                    line = line.replace('XX_DOCKER_VERSION_XX', docker_version)
                elif 'XX_INPUT_TABLE_XX' in line:
                    line = line.replace('XX_INPUT_TABLE_XX', input_table_name)
                elif 'XX_S3_DIR_XX' in line:
                    line = line.replace('XX_S3_DIR_XX', s3_dir)

                job_slurm.write(line)
            job_slurm.close()
            f_open.close()


def build_json_input(args, refdir, inpdir):
    download_ref = list()
    job_json = os.path.join(refdir, args.output_uuid + '.json')
    # config_file = os.path.join(args.basedir, config_file)

    # output_vcf = output_uuid + '.vcf'
    f_open = open(job_json, 'w')
    with open("topmed_cwl/slurm/etc/job_template.json", 'r') as bam_open:
        for line in bam_open:
            if 'XX_refdir_XX' in line:
                newline = line.replace('XX_refdir_XX', refdir)
                f_open.write(newline)
                file = line.split("/")[1]
                if file not in download_filelist:
                    download_ref.append(file)
            elif 'XX_INPUT_BAM_XX' in line:
                newline = line.replace('XX_INPUT_BAM_XX', os.path(inpdir, os.path.basename(args.s3_url)))
                f_open.write(newline)
            elif 'XX_OUTPUT_BAMNAME_XX' in line:
                newline = line.replace('XX_OUTPUT_BAMNAME_XX', args.output_basename)
                f_open.write(newline)
            else:
                f_open.write(line)
    bam_open.close()
    f_open.close()

    return download_ref


def run_cwl(args, statusclass, metricsclass):

    output_uuid = args.output_uuid
    input_id = args.input_id
    ref_table_name = args.ref_table_name
    input_table_name = args.input_table_name
    project = args.project
    cwl_veriosn = args.cwl_version
    docker_version = args.docker_version
    s3_url = args.s3_url
    s3_profile = args.s3_profile
    s3_endpoint = args.s3_endpoint
    md5 = args.md5

    # create directory structure and input locations
    jobdir = tempfile.mkdtemp(prefix="bam_%s" % output_uuid, dir="./")
    workdir = tempfile.mkdtemp(prefix="workdir_%s" % output_uuid, dir=jobdir)
    inpdir = tempfile.mkdtemp(prefix="input_%s" % output_uuid, dir=jobdir)
    refdir = tempfile.mkdtemp(prefix="ref_%s" % output_uuid, dir=jobdir)
    # local_paths = os.path.join(inp, os.path.basename(args.s3_url))

    # get hostname and datetime
    hostname = socket.gethostname()
    datetime_start = str(datetime.datetime.now())

    # setup logger
    log_file = os.path.join(workdir, "%s.alignment.cwl.log" % str(output_uuid))
    logger = utils.pipeline.setup_logging(output_uuid, log_file)

    # logging inputs
    logger.info("bam_input_id: %s" % (input_id))
    logger.info("cram_id: %s" % (output_uuid))
    logger.info("hostname: %s" % (hostname))
    logger.info("datetime_start: %s" % (datetime_start))

    # Get CWL start time
    cwl_start = time.time()

    # Connect to database
    # pg_file = inputs['db_config']['location']
    engine = postgres.utils.get_db_engine(args)
    refs = postgres.status.get_bams(engine, ref_table_name)

    # Download reference files
    logger.info("Downloading reference")
    for ref in refs:
        local_ref = os.path(refdir, os.path.basename(ref.s3_url))
        download_exit_code = utils.s3.aws_s3_get(logger, ref.s3_url, local_ref, ref.profile, ref.endpoint, recursive=False)
        ref_md5 = utils.get_md5(local_ref)
        download_ref_end_time = time.time()
        download_time = download_ref_end_time - cwl_start
        if download_exit_code == 0 and ref_md5 == ref.md5sum:
            logger.info("Download reference %s successfully in %s" % (ref.s3_url, local_ref))
        else:
            cwl_elapsed = download_time
            datetime_end = str(datetime.datetime.now())
            postgres.utils.set_download_error(download_exit_code, logger, engine, project, input_id, ref.input_id, input_table_name, output_uuid, datetime_start, datetime_end, hostname, cwl_version, docker_version, download_time, cwl_elapsed, statusclass, metricsclass)
            # Exit
            sys.exit(download_exit_code)

    # Download input bam file
    logger.info("Downloading input bam file")
    local_input = os.path(input_dir, os.path.basename(s3_url))
    download_exit_code = utils.s3.aws_s3_get(logger, s3_url, local_input, s3_profile, s3_endpoint, recursive=False)
    download_input_end_time = time.time()
    download_time = download_input_end_time - download_ref_end_time

    if download_exit_code == 0 and input_md5 == md5:
        logger.info("Download input %s successfully in %s" % (s3_url, local_input))
    else:
        cwl_elapsed = download_input_end_time - cwl_start
        datetime_end = str(datetime.datetime.now())
        postgres.utils.set_download_error(download_exit_code, logger, engine, project, input_id, input_id, input_table_name, output_uuid, datetime_start, datetime_end, hostname, cwl_version, docker_version, download_time, cwl_elapsed, statusclass, metricsclass)
        # Exit
        sys.exit(download_exit_code)

    # Run CWL
    json_file = build_json_input(args, refdir, inpdir)
    os.chdir(workdir)
    logger.info('Running CWL workflow')
    cmd = ['/usr/bin/time', '-v',
           '/home/ubuntu/.virtualenvs/p2/bin/cwltool',
           "--debug",
           "--relax-path-checks",
           "--tmpdir-prefix", inputdir,
           "--tmp-outdir-prefix", workdir,
           args.cwl,
           json_file]

    cwl_exit = utils.pipeline.run_command(cmd, logger)

    # define outputs
    output_cram = os.path.join(workdir, args.output_basename + '_BQSR.sort.cram')
    output_crai = output_cram.replace("cram", "crai")

    # Get md5 and file size
    output_md5 = utils.pipeline.get_md5(output_bam)
    output_file_size = utils.pipeline.get_file_size(output_bam)

    # Upload output
    upload_start = time.time()
    os.chdir(jobdir)
    upload_dir_location = os.path.join(args.s3dir, args.output_id)
    upload_cram_location = os.path.join(upload_dir_location, os.path.basename(output_cram))
    upload_crai_location = os.path.join(upload_dir_location, os.path.basename(output_crai))
    upload_log_location = os.path.join(upload_dir_location, os.path.basename(log_file))
    logger.info("Uploading workflow output to %s" % (upload_cram_location))
    upload_exit = utils.s3.aws_s3_put(logger, upload_cram_location, output_cram, s3_profile, s3_endpoint, recursive=False)
    upload_exit = utils.s3.aws_s3_put(logger, upload_crai_location, output_crai, s3_profile, s3_endpoint, recursive=False)

    # Calculate times
    cwl_end = time.time()
    upload_time = cwl_end - upload_start
    cwl_elapsed = cwl_end - cwl_start
    datetime_end = str(datetime.datetime.now())
    logger.info("datetime_end: %s" % (datetime_end))

    # Get status info
    logger.info("Get status/metrics info")
    status, loc = postgres.status.get_status(upload_exit, cwl_exit, upload_cram_location, upload_dir_location, logger)

    # Set status table
    logger.info("Updating status: %d" % cwl_exit)
    postgres.utils.add_pipeline_status(engine, project, input_id, input_id, input_table_name, output_uuid, status,
                                       loc, datetime_start, datetime_end, md5, file_size, hostname, cwl_version, docker_version, statusclass)

    # Get metrics info
    time_metrics = utils.pipeline.get_time_metrics(log_file)

    # Set metrics table
    logger.info("Updating metrics")
    postgres.utils.add_pipeline_metrics(engine, input_id, input_id, download_time,
                                        upload_time, args.nthreads, cwl_elapsed,
                                        sum(time_metrics['system_time']) / float(len(time_metrics['system_time'])),
                                        sum(time_metrics['user_time']) / float(len(time_metrics['user_time'])),
                                        sum(time_metrics['wall_clock']) / float(len(time_metrics['wall_clock'])),
                                        sum(time_metrics['percent_of_cpu']) / float(len(time_metrics['percent_of_cpu'])),
                                        sum(time_metrics['maximum_resident_set_size']) / float(len(time_metrics['maximum_resident_set_size'])),
                                        status, metricsclass)

    # Remove job directories, upload final log file
    logger.info("Uploading main log file")
    utils.s3.aws_s3_put(logger, os.path.join(upload_dir_location, os.path.basename(log_file)), log_file, s3_profile, s3_endpoint, recursive=False)
    utils.pipeline.remove_dir(os.path.dirname(args.base_dir))


if __name__ == '__main__':
    # Get args
    args = get_args()
    # Run tool
    if args.choice == 'slurm':
        run_build_slurm_scripts(args)
    elif args.choice == 'run_cwl':
        class TableStatus(postgres.mixins.StatusTypeMixin, postgres.utils.Base):
            __tablename__ = "alignment_cwl_status"

        class TableMetrics(postgres.mixins.MetricsTypeMixin, postgres.utils.Base):
            __tablename__ = "alignment_cwl_metrics"
    # Run CWL
    run_cwl(args, TableStatus, TableMetrics)
