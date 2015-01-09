#!/usr/bin/env bash
doc_file_name="datasets/20news.dat"

num_vocabs=53975
alpha_sum=10
beta=0.1
num_topics=100
num_iterations=100
compute_ll_interval=2
output_prefix="lightLDA" 
dump_iter=10
decay_step=100
gs_type=0
mh_step=1 
log_dirname=log

# Figure out the paths.
doc_file=$(readlink -f $doc_file_name)
script_path=`readlink -f $0`
script_dir=`dirname $script_path`
app_dir=`dirname $script_dir`
progname=lda_st
prog_path=$app_dir/bin/$progname
log_path=${app_dir}/$log_dirname

# Mkdir
if [ ! -d "$log_path" ]; then
    mkdir -p $log_path
fi

cmd="GLOG_logtostderr=true \
    GLOG_log_dir=$log_path \
    GLOG_v=-1 \
    GLOG_minloglevel=0 \
    GLOG_vmodule= \
    $prog_path \
    --doc_file $doc_file \
    --num_vocabs $num_vocabs \
    --alpha_sum $alpha_sum \
    --beta $beta \
    --num_topics $num_topics \
    --num_iterations $num_iterations \
    --compute_ll_interval $compute_ll_interval \
    --output_prefix $output_prefix \
    --dump_iter $dump_iter \
    --decay_step $decay_step \
    --gs_type $gs_type \
    --mh_step $mh_step &"
# run on local machine
eval $cmd
