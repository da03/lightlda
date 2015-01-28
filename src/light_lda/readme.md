LightLDA

1.Introduction

LightLDA is a distributed system for large-scale topic modeling. It can train a big Latent Dirichlet Allocation(LDA) model with trillions of parameters on web-scale corpus with hundreds of billions of tokens using a modest compute cluster. LightLDA implements the distributed version of our recently-proposed O(1) sampler through a data- and model- parallelism architecture.

The key features of LightLDA are:

* Biggest training data size ever reported through data parallelism and out-of-core storage
* Biggest model size ever reported through model parallelism and data-model-slice mechanism
* Highly performance-efficient through our proposed O(1) sampler
* Highly cost-efficient with as few as 8 machines for such a big data and big model through careful system implementation

The name of 'Light'LDA comes from the fact that, 1) LightLDA solves the sampling bottleneck by novel algorithmic innovation, which is agnostic of model size. The bottleneck of new sampler is memory access, which means we can train lda (almost) with speed of light; 2) LightLDA gives a lightweight implementation, making large-scale topic modeling on modest hardware resources possible. 

2. Installation

LightLDA is built and tested on Windows platform. It uses several third-party open source libraries.

* zeromq
* glog
* gflags
* boost

3. Data preprocess

Before running LightLDA, you should prepare data to particular format. LightLDA also offers the following preprocess tools.

dump_binary_doc_mn.exe:
	input: LibSVM format of data
	output: binary format of data block and local vocabulary

dump_dict_meta_mn.exe
	input: global vocabulary
	output: meta information of model block

4. How to Run

you can run LightLDA with the command:

./light_lda.exe --hostfile=D:\localserver.txt --num_clients=1 --client_id=0 --num_worker_threads=32 --num_delta_threads=2 --cold_start=false --staleness=1 --num_blocks=2 --block_offset=0 --doc_file=D:\data\block --vocab_file=D:\data\vocab --dump_file=D:\data --meta_name=D:\data\meta.0 --alpha=0.0001 --beta=0.01 --mh_step=8 --num_vocabs=50000 --num_topics=1000000 --num_iterations=10 --compute_ll_interval=1 --dump_model_interval=2 --block_size=30000000 --block_max_capacity=8000000000 --model_max_capacity=9000000000 --alias_max_capacity=9000000000 --delta_max_capacity=1000000000 --load_factor=2

you can run ./light_lda.exe --help to get a intuitive explaination for each argument.

Next we will give more deeply instruction for preparing data and arguments

System Arguments

3.1 hostfile

LightLDA is built on Petuum Parameter Server thus use the same format of configuration files:

0 ip_address_0 10000
1 ip_address_0 9999
1000 ip_address_1 9999
2000 ip_address_2 9999
3000 ip_address_3 9999
...

Each line in the server configuration file format specifies an ID (0, 1, 1000, 2000, etc.), the IP address of the machine assigned to that ID, and a port number (9999 or 10000). Every machine is assigned to one ID and one port, except for the first machine, which is assigned two IDs and two ports because it has a special role.

To run the LightLDA in single machine, you can simply create this localhost configuration file as:

0 127.0.0.1 9999
1 127.0.0.1 10000

When training LDA with a single machine, we strongly suggest running single-node multi-threads version of light lda, instead just running distributed LightLDA with single node configuration, because the design for optimizing network communication may result in performance penalty.

3.2 cold_start

if cold_start=true, system will randomly initialize the topic assignment and train from scratch.
if cold_start=false, system will warm start: initialize the model based on the state previously stored in disk, and continuously trained with last time.
This option can be used for fault tolerent.

3.3 staleness

if staleness equals to 1, the system would run in BSP mode.
If staleness equals to number of iterations, the system would run in ASP mode.
Otherwise, system will run in SSP.

Data Arguments

For example, in 