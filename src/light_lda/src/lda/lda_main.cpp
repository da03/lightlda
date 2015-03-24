// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2014.03.25

#include <thread>
#include <vector>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "lda/lda_engine.hpp"
#include "system/host_info.hpp"
#include "system/table_group.hpp"
#include "util/utils.hpp"

// System Parameters
DEFINE_string(hostfile, "", "Path to file containing server ip:port.");
DEFINE_int32(num_clients, 1, "Total number of clients");
DEFINE_int32(client_id, 0, "Client ID");
DEFINE_int32(num_worker_threads, 1, "Number of app threads in this client");
DEFINE_int32(num_delta_threads, 1, "Number of delta threads in this client");
//DEFINE_int32(num_server_threads, 1, "Number of server threads in this client");
DEFINE_bool(cold_start, true, "cold start or warm start");
DEFINE_int32(staleness, 0, "staleness for SSP");

// Input data Parameters
DEFINE_int32(num_blocks, 1, "Number of blocks of training data");
DEFINE_int32(block_offset, 0, "id of first block in this client");
DEFINE_string(doc_file, "", "data block file name");
DEFINE_string(vocab_file, "", "local vocabulary file name");
DEFINE_string(dump_file, "", "");
DEFINE_string(meta_name, "", "dictionary meta file name");

// LDA Parameters
DEFINE_double(alpha, 0.01, "Dirichlet prior on document-topic vectors.");
DEFINE_double(beta, 0.01, "Dirichlet prior on vocab-topic vectors.");
DEFINE_int32(mh_step, 1, "number of Metropolis Hastings step");
DEFINE_int32(num_vocabs, -1, "Number of vocabs.");
DEFINE_int32(num_topics, 100, "Number of topics.");
DEFINE_int32(num_iterations, 10, "Number of iterations");
DEFINE_int32(compute_ll_interval, -1, "Copmute log likelihood over local dataset on every N iterations");
DEFINE_int32(dump_model_interval, -1, "Dump out model on every N iterations");

// Pre-allocate memory Parameter
DEFINE_int32(block_size, 1000000, "the maximum number of docs in each block");
DEFINE_int64(block_max_capacity, 0, "size of one data block");
DEFINE_int64(model_max_capacity, 0, "size of one slice model table");
DEFINE_int64(alias_max_capacity, 0, "size of one slice alias table");
DEFINE_int64(delta_max_capacity, 0, "size of one slice delta table");
DEFINE_int32(load_factor, 5, "load factor of light weight hash table");

int main(int argc, char *argv[]) {
	google::ParseCommandLineFlags(&argc, &argv, true);
	google::InitGoogleLogging(argv[0]);


	// PS configuration
	petuum::TableGroupConfig table_group_config;

	// 1 server thread per client
	table_group_config.num_total_server_threads = FLAGS_num_clients;
	// 1 background thread per client
	table_group_config.num_total_bg_threads = FLAGS_num_clients;
	table_group_config.num_total_clients = FLAGS_num_clients;

	// doc-topic table, summary table, llh table.
	// table_group_config.num_tables = 3;
	table_group_config.num_local_server_threads = 1;
	// + 1 for main() thread.
	table_group_config.num_local_app_threads = FLAGS_num_worker_threads + 1;
	//table_group_config.num_local_app_threads = FLAGS_num_worker_threads + 2;
	table_group_config.num_local_bg_threads = 1;

	petuum::GetHostInfos(FLAGS_hostfile, &table_group_config.host_map);
	petuum::GetServerIDsFromHostMap(&(table_group_config.server_ids),
		table_group_config.host_map);

	table_group_config.client_id = FLAGS_client_id;
	table_group_config.consistency_model = petuum::SSPPush;

	// Global LDA configuration
	table_group_config.num_vocabs = FLAGS_num_vocabs;
	table_group_config.num_topics = FLAGS_num_topics;
	table_group_config.meta_name = FLAGS_meta_name;
	table_group_config.dump_file = FLAGS_dump_file;
	table_group_config.dump_iter = FLAGS_dump_model_interval;

	int32_t init_thread_id = petuum::TableGroup::Init(table_group_config, false);
	LOG(INFO) << "Initialized TableGroup, init thread id = " << init_thread_id;

	LOG(INFO) << "num of clients = " << FLAGS_num_clients;
	LOG(INFO) << "client id = " << FLAGS_client_id;
	LOG(INFO) << "alpha = " << FLAGS_alpha;
	LOG(INFO) << "beta = " << FLAGS_beta;
	LOG(INFO) << "num_topics = " << FLAGS_num_topics;
	LOG(INFO) << "mh_step = " << FLAGS_mh_step;
	LOG(INFO) << "staleness = " << FLAGS_staleness;
	LOG(INFO) << "cold_start = " << FLAGS_cold_start;

	LOG(INFO) << "Starting LDA with " << FLAGS_num_worker_threads << " threads "
		<< "on client " << FLAGS_client_id;
	lda::LDAEngine lda_engine;

	lda_engine.Setup();

	std::vector<std::thread> threads(FLAGS_num_worker_threads);
	for (auto& thr : threads) {
		thr = std::thread(&lda::LDAEngine::Train, std::ref(lda_engine));
	}
	for (auto& thr : threads) {
		thr.join();
	}

	LOG(INFO) << "LDA finished!";
	petuum::TableGroup::ShutDown();
	LOG(INFO) << "LDA shut down!";
	return 0;
}
