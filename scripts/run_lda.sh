#!/usr/bin/env bash
hostfile_name="scripts/localserver"
num_worker_threads=12
num_delta_threads=2
cold_start=true
staleness=1
num_blocks=51
block_offset=0
doc_file_name="../cw_ten_percent/datasets/binary_doc_dir_T300k_B30m/block"
vocab_file_name="../cw_ten_percent/datasets/binary_doc_dir_T300k_B30m/vocab"
dump_dir="output/"
meta_file_name_prefix="../cw_ten_percent/meta" # do not include .client_id
alpha=0.0005
beta=0.01
mh_step=2
num_vocabs=1000000
num_topics=300000
num_iterations=5000
compute_ll_interval=1
dump_model_interval=10
block_size=1000001
block_max_capacity=3000000000
model_max_capacity=2000000000
alias_max_capacity=2000000000
delta_max_capacity=200000000
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

# Spawn program instances
client_id=0
client_block_offset=$block_offset
ave_block_num=$((num_blocks / num_unique_hosts))

for ip in $unique_host_list; do
  echo Running client $client_id on $ip
  
  if [ "$((client_block_offset + ave_block_num))" -ge "$((num_blocks + block_offset))" ]
  then
      client_num_blocks=$((num_blocks - client_block_offset))
  else
      if [ "$client_id" -eq "$((num_unique_hosts - 1))" ]
      then
          client_num_blocks=$((num_blocks - client_block_offset))
      else
          client_num_blocks=$ave_block_num
      fi
  fi
  meta_name="${app_dir}/${meta_file_name_prefix}.${client_id}"
  cmd="GLOG_logtostderr=false \
      GLOG_log_dir=$log_path \
      GLOG_v=-1 \
      GLOG_minloglevel=0 \
      GLOG_vmodule= \
      $prog_path \
      --hostfile $hostfile \
      --num_clients $num_unique_hosts \
      --client_id $client_id \
      --num_worker_threads $num_worker_threads \
      --num_delta_threads $num_delta_threads \
      --staleness $staleness \
      --num_blocks $client_num_blocks \
      --block_offset $client_block_offset \
      --doc_file $doc_file \
      --vocab_file $vocab_file \
      --dump_file $dump_path \
      --meta_name $meta_name \
      --alpha $alpha \
      --beta $beta \
      --mh_step $mh_step \
      --num_vocabs $num_vocabs \
      --num_topics $num_topics \
      --num_iterations $num_iterations \
      --compute_ll_interval $compute_ll_interval \
      --dump_model_interval $dump_model_interval \
      --block_size $block_size \
      --block_max_capacity $block_max_capacity \
      --model_max_capacity $model_max_capacity \
      --alias_max_capacity $alias_max_capacity \
      --delta_max_capacity $delta_max_capacity \
      --load_factor $load_factor \
      --$flag_cold_start"
  ssh $ssh_options $ip $cmd & 
  #eval $cmd  # Use this to run locally (on one machine).

  # Wait a few seconds for the name node (client 0) to set up
  if [ $client_id -eq 0 ]; then
    echo $cmd   # echo the cmd for just the first machine.
    echo "Waiting for name node to set up..."
    sleep 3
  fi
  echo $cmd
  client_id=$(( client_id+1 ))
  client_block_offset=$(( client_block_offset+client_num_blocks ))
done
