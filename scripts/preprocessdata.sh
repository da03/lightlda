#!/usr/bin/env bash
#hostfile_name="scripts/localserver"
log_dirname="log/"
word_id_filename="../cw_ten_percent/word_dict.id.txt"
dict_meta_filename="../cw_ten_percent/meta"
load_factor=2
num_vocabs=1000000
num_topics=300000
num_clients=4

libsvm_doc="../cw_ten_percent/cw_1M_libsvm.conv"
binary_doc_dirname="../cw_ten_percent/datasets/binary_doc_dir_T300k_B30m"
block_size=1000000
mean_doc_size=233
word_tf_filename=$word_id_filename
file_offset=0
# Figure out the paths.
#hostfile=$(readlink -f $hostfile_name)
word_id_file=$(readlink -f $word_id_filename)
word_tf_file=$(readlink -f $word_tf_filename)
script_path=`readlink -f $0`
script_dir=`dirname $script_path`
app_dir=`dirname $script_dir`
progname=dump_dict_meta_mn
dict_meta_file=${app_dir}/$dict_meta_filename
prog_path=$app_dir/bin/$progname
log_path=${app_dir}/$log_dirname
binary_doc_dir=${app_dir}/$binary_doc_dirname

# Mkdir
if [ ! -d "$log_path" ]; then
    mkdir -p $log_path
fi
if [ ! -d "$binary_doc_dir" ]; then
    mkdir -p $binary_doc_dir
fi

## Parse hostfile
#host_list=`cat $hostfile | awk '{ print $2 }'`
#unique_host_list=`cat $hostfile | awk '{ print $2 }' | uniq`
#num_unique_hosts=`cat $hostfile | awk '{ print $2 }' | uniq | wc -l`
#ip=${unique_host_list[0]}
#echo Running preprocessing on $ip
#ssh $ssh_options $ip
echo Running preprocessing on local machine
cmd="GLOG_logtostderr=false \
    GLOG_log_dir=$log_path \
    GLOG_v=-1 \
    GLOG_minloglevel=0 \
    GLOG_vmodule= \
    $prog_path \
    --word_id_file $word_id_file \
    --dict_meta_file $dict_meta_file \
    --load_factor $load_factor \
    --num_vocabs $num_vocabs \
    --num_topics $num_topics \
    --num_clients $num_clients"
eval $cmd  # Use this to run locally (on one machine).
echo $progname done
progname=dump_binary_doc_mn
prog_path=$app_dir/bin/$progname
cmd="GLOG_logtostderr=false \
    GLOG_log_dir=$log_path \
    GLOG_v=-1 \
    GLOG_minloglevel=0 \
    GLOG_vmodule= \
    $prog_path \
    --libsvm_doc $libsvm_doc \
    --binary_doc_dir $binary_doc_dir \
    --block_size $block_size \
    --mean_doc_size $mean_doc_size \
    --word_tf_file $word_tf_file \
    --file_offset $file_offset"
eval $cmd
echo $progname done
