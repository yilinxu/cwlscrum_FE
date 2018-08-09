#!/usr/bin/env bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --workdir="/mnt/SCRATCH/"
#SBATCH --cpus-per-task=XX_CORE_COUNT_XX
#SBATCH --mem=XX_MEM_XX
#SBATCH --gres=SCRATCH:XX_DISK_GB_XX

function cleanup (){
    echo "cleanup tmp data";
    sudo rm -rf $basedir;
}

input_id="XX_INPUTID_XX"
output_uuid="XX_OUTPUTID_XX"
output_basename = "XX_OUTPUTBASE_XX"
project="XX_PROJECT_XX"
output_id="XX_OUTPUT_ID_XX"
md5="XX_MD5_XX"
s3_url="XX_S3URL_XX"
s3_profile="XX_S3PROFILE_XX"
s3_endpoint="XX_S3ENDPOINT_XX"
thread_count="XX_THREAD_COUNT_XX"
pg_useer="XX_PG_USER_XX"
pg_pw="XX_PG_PW_XX"
ref_table_name="XX_REF_TABLE_XX"
cwl_version="XX_CWL_VERSION_XX"
docker_version="XX_DOCKER_VERSION_XX"
input_table_name = "XX_INPUT_TABLE_XX"
s3_dir = "XX_S3_DIR_XX"

basedir=`sudo mktemp -d topmed.XXXX -p /mnt/SCRATCH/`

repository="git@github.com:yilinxu/cwlscrum_topmed.git"
sudo chown ubuntu:ubuntu $basedir

cd $basedir

sudo git clone -b master $repository topmed_cwl
sudo chown ubuntu:ubuntu -R topmed_cwl

trap cleanup EXIT

/home/ubuntu/.virtualenvs/p2/bin/python topmed_cwl/slurm/alignment-run-workflow.py run_cwl \
--input_id $input_id \
--output_uuid $output_uuid \
--output_basename $output_basename
--project $project \
--md5 $md5 \
--s3_url $s3_url \
--s3_profile $s3_profile \
--s3_endpoint $s3_endpoint \
--cwl topmed_cwl/workflows/alignment_workflow_md5checker.cwl \
--thread_count $thread_count
--pg_useer $pg_user
--pg_pw $pg_pw
--ref_table_name $ref_table_name
--cwl_version $cwl_version
--docker_version $docker_version
--input_table_name $input_table_name
--s3_dir $s3_dir
--base_dir $basedir
