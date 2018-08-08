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
#import os
#import sys


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
    p_slurm.add_argument('--postgres_config', required=True)
    p_slurm.add_argument('--refdir', required=True)
    p_slurm.add_argument('--s3dir_bam', required=True)

    # Build json files and run cwl
    p_input = sp.add_parser('run_cwl', help='Options for building input json and run cwl.')
    p_input.add_argument('--case_id', required=True)
    p_input.add_argument('--tumor_id', required=True)
    p_input.add_argument('--normal_id', required=True)
    p_input.add_argument('--tumor_s3url', required=True)
    p_input.add_argument('--normal_s3url', required=True)
    p_input.add_argument('--output_uuid', required=True)
    p_input.add_argument('--s3_load_bucket', required=True)
    p_input.add_argument('--json_template', default='job_template.json')
    p_input.add_argument('--basedir', required=True)
    p_input.add_argument('--cwl_runner', required=True)
    p_input.add_argument('--nthreads', required=True)
    p_input.add_argument('--blocks', required=True)
    p_input.add_argument('--cwl_merge', required=False)
    p_input.add_argument('--cwl_sort', required=False)
    p_input.add_argument('--cwl_version', default="1.0.20170828135420", required=False)
    p_input.add_argument('--docker_version', default="quay.io/ncigdc/pindel-tool:1.4", required=False)

    return parser.parse_args()


def run_build_slurm_scripts(args):
    # Slurm template, downloaded files and output path
    slurm_script_path = args.slurm_script_path
    slurm_template_path = args.slurm_template_path
    refdir = args.refdir
    s3dir_bam = args.s3dir_bam

    # Postgres configuration
    input_table_name = args.input_table_name
    postgres_config = args.postgres_config

    # Slurm resources and configuration
    resource_core_count = args.resource_core_count
    disk_gigabytes = args.resource_disk_gb
    memory_gigabytes = args.resource_memory_gb
    thread_count = args.thread_count

    # Get list of bams from input table
    engine = postgres.utils.get_db_engine(postgres_config)
    bams = postgres.status.get_bams(engine, input_table_name)

    for bam in bams:
        output_uuid = str(uuid.uuid4())
        input_id = bam.input_id
        project = bam.project
        md5 = bam.md5sum
        s3_url = bam.s3_url
        s3_profile = bam.s3_profile
        s3_endpoint = bam.s3_endpoint

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
                elif "XX_BAM_S3DIR_XX" in line:
                    line = line.replace("XX_BAM_S3DIR_XX", args.s3dir)
                elif "XX_OUTPUT_ID_XX" in line:
                    line = line.replace("XX_OUTPUT_ID_XX", output_uuid)
                elif 'XX_CORE_COUNT_XX' in line:
                    line = line.replace('XX_CORE_COUNT_XX', str(resource_core_count))
                elif 'XX_DISK_GB_XX' in line:
                    line = line.replace('XX_DISK_GB_XX', str(disk_gigabytes))
                elif 'XX_REFDIR_XX' in line:
                    line = line.replace('XX_DISK_GB_XX', refdir)
                elif 'XX_DISK_GB_XX' in line:
                    line = line.replace('XX_DISK_GB_XX', s3dir_bam)

                job_slurm.write(line)
            job_slurm.close()
            f_open.close()


if __name__ == '__main__':
    # Get args
    args = get_args()
    # Run tool
    if args.choice == 'slurm':
        run_build_slurm_scripts(args)
    elif args.choice == 'run_cwl':
        class TableStatus(postgres.mixins.StatusTypeMixin, postgres.utils.Base)
            __tablename__ = "alignment_cwl_status"

        class TableMetrics(postgres.mixins.MetricsTypeMixin, postgres.utils.Base)
            __tablename__ = "alignment_cwl_metrics"
    # Run CWL
    run_cwl(args, TableStatus, TableMetrics)
