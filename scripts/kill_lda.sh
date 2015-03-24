#!/usr/bin/env bash
hostfile_name="scripts/localserver"
num_worker_threads=8
num_delta_threads=1
cold_start=true
staleness=1
num_blocks=12
block_offset=0
doc_file_name="datasets/binary_doc_dir_T100k_B100k/block"
vocab_file_name="datasets/binary_doc_dir_T100k_B100k/vocab"
dump_dir="output/"
meta_file_name_prefix="datasets/featurefile.meta" # do not include .client_id
alpha=0.1
beta=0.1
mh_step=2
num_vocabs=1998992
num_topics=100000
num_iterations=5000
compute_ll_interval=3
dump_model_interval=12
block_size=30000001
block_max_capacity=800000000
model_max_capacity=8000000000
alias_max_capacity=8000000000
delta_max_capacity=3000000000
load_factor=2
log_dirname="log/"
# Figure out the paths.
doc_file=$(readlink -f $doc_file_name)
vocab_file=$(readlink -f $vocab_file_name)
meta_dir=`dirname $meta_file_name_prefix`
hostfile=$(readlink -f $hostfile_name)
script_path=`readlink -f $0`
script_dir=`dirname $script_path`
app_dir=`dirname $script_dir`
progname=lda_main
prog_path=$app_dir/bin/$progname
log_path=${app_dir}/$log_dirname
dump_file=${app_dir}/$dump_dir
dump_path="${dump_file}/dump"
# Mkdir
if [ ! -d "$log_path" ]; then
    mkdir -p $log_path
fi
if [ ! -d "$dump_file" ]; then
    mkdir -p $dump_file
fi

# Deal with booleans
if [ "$cold_start" = true ]; then
    flag_cold_start="cold_start"
else
    flag_cold_start="nocold_start"
fi 
# Parse hostfile
host_list=`cat $hostfile | awk '{ print $2 }'`
unique_host_list=`cat $hostfile | awk '{ print $2 }' | uniq`
num_unique_hosts=`cat $hostfile | awk '{ print $2 }' | uniq | wc -l`

# Kill previous instances of this program
echo "Killing previous instances of '$progname' on servers, please wait..."
for ip in $unique_host_list; do
  ssh $ssh_options $ip \
    killall -q $progname
done
echo "All done!"
