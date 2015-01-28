#include "system/table_group.hpp"
#include <algorithm>
#include <iostream>
#include "system/system_context.hpp"
#include "system/server_threads.hpp"
#include "system/name_node_thread.hpp"
#include "system/bg_workers.hpp"
#include "util/stats.hpp"


namespace petuum {

	pthread_barrier_t TableGroup::register_barrier_;
	std::atomic<int> TableGroup::num_app_threads_registered_;
	VectorClockMT TableGroup::vector_clock_;
	bool TableGroup::server_init_finish_;

	util::MtQueueMove<std::unique_ptr<ServerPushOpLogIterationMsg>> TableGroup::server_delta_queue_;

	int32_t TableGroup::Init(const TableGroupConfig &table_group_config,
		bool table_access) {

		int32_t num_total_server_threads
			= table_group_config.num_total_server_threads;
		int32_t num_local_server_threads
			= table_group_config.num_local_server_threads;
		int32_t num_local_app_threads = table_group_config.num_local_app_threads;


		//int32_t num_local_table_threads = table_access ? num_local_app_threads  : (num_local_app_threads - 1);

		// NOTE(jiyuan): hack here, we assume the num_local_table_threads 
		// to be the number of threads which need to communicat with bg_worker.
		// in our case, only ModelIOThread and DeltaIOThread need to connect to
		// bg_worker
		// 2 + 1, plus the Init main thread
		int32_t num_local_table_threads = 3;
		int32_t num_local_io_threads = 2;
        pthread_barrier_init(&register_barrier_, NULL, 2);
		// NOTE(v-feigao): modified to allowing multiple delta io threads.
		// num_delta_threads * DeltaIOThread + 1 ModelIOThread
		// plus 1 the Init main thread
		//int32_t num_local_io_threads = table_group_config.num_delta_threads + 1;
		//int32_t num_local_table_threads = num_local_io_threads + 1;

		int32_t num_local_bg_threads = table_group_config.num_local_bg_threads;
		int32_t num_total_bg_threads = table_group_config.num_total_bg_threads;
		int32_t num_tables = table_group_config.num_tables;
		int32_t num_total_clients = table_group_config.num_total_clients;
		const std::vector<int32_t> &server_ids = table_group_config.server_ids;
		const std::map<int32_t, HostInfo> &host_map = table_group_config.host_map;

		int32_t client_id = table_group_config.client_id;
		int32_t server_ring_size = table_group_config.server_ring_size;
		ConsistencyModel consistency_model = table_group_config.consistency_model;
		int32_t local_id_min = GlobalContext::get_thread_id_min(client_id);
		int32_t local_id_max = GlobalContext::get_thread_id_max(client_id);
		num_app_threads_registered_ = 1;  // init thread is the first one

		// can be Inited after CommBus but must be before everything else
		GlobalContext::Init(num_total_server_threads,
			num_local_server_threads,
			num_local_app_threads,
			num_local_table_threads,
			num_local_io_threads,
			num_local_bg_threads,
			num_total_bg_threads,
			num_tables,
			num_total_clients,
			server_ids,
			host_map,
			client_id,
			server_ring_size,
			consistency_model,
			table_group_config.aggressive_cpu,
			table_group_config.num_vocabs,
			table_group_config.num_topics,
			table_group_config.meta_name,
			table_group_config.dump_file,
			table_group_config.dump_iter);

		CommBus *comm_bus = new CommBus(local_id_min, local_id_max, 1);
		GlobalContext::comm_bus = comm_bus;

		int32_t init_thread_id = local_id_min
			+ GlobalContext::kInitThreadIDOffset;
		CommBus::Config comm_config(init_thread_id, CommBus::kNone, "");

		GlobalContext::comm_bus->ThreadRegister(comm_config);

		if (GlobalContext::am_i_name_node_client()) {
			NameNodeThread::Init();
			ServerThreads::Init(local_id_min + 1);
		}
		else {
			ServerThreads::Init(local_id_min);
		}

		BgWorkers::Init(&server_delta_queue_);

		ThreadContext::RegisterThread(init_thread_id);

		if (table_access)
			vector_clock_.AddClock(init_thread_id, 0);
		server_init_finish_ = false;

		return init_thread_id;
	}

	void TableGroup::ShutDown() {
		pthread_barrier_destroy(&register_barrier_);
		BgWorkers::ThreadDeregister();
		ServerThreads::ShutDown();

		if (GlobalContext::am_i_name_node_client())
			NameNodeThread::ShutDown();

		BgWorkers::ShutDown();
		GlobalContext::comm_bus->ThreadDeregister();

		delete GlobalContext::comm_bus;
		//for(auto iter = tables_.begin(); iter != tables_.end(); iter++){
		//  delete iter->second;
		//}
		PRINT_STATS();
	}


	void TableGroup::WaitThreadRegister(){
		VLOG(0) << "num_table_threads = "
			<< GlobalContext::get_num_table_threads()
			<< " num_app_threads = "
			<< GlobalContext::get_num_app_threads();

		if (GlobalContext::get_num_table_threads() ==
			GlobalContext::get_num_app_threads()) {
			VLOG(0) << "Init thread WaitThreadRegister";
			pthread_barrier_wait(&register_barrier_);
		}
	}

	int32_t TableGroup::RegisterThread(){
		REGISTER_THREAD_FOR_STATS(true);
		int app_thread_id_offset = num_app_threads_registered_++;

		int32_t thread_id = GlobalContext::get_local_id_min()
			+ GlobalContext::kInitThreadIDOffset + app_thread_id_offset;

        std::cout<<"-----here-----" << thread_id<<std::endl<<std::flush;

		petuum::CommBus::Config comm_config(thread_id, petuum::CommBus::kNone, "");
		GlobalContext::comm_bus->ThreadRegister(comm_config);

		ThreadContext::RegisterThread(thread_id);

		BgWorkers::ThreadRegister();
		vector_clock_.AddClock(thread_id, 0);


		pthread_barrier_wait(&register_barrier_);
        std::cout<<"-----endhere-----" << thread_id<<std::endl<<std::flush;
		return thread_id;
	}

	void TableGroup::DeregisterThread(){
		FINALIZE_STATS();
		BgWorkers::ThreadDeregister();
		GlobalContext::comm_bus->ThreadDeregister();
	}

	//bool TableGroup::IsServerInitFinish() {
	//	return server_init_finish_;
	//}
	//void TableGroup::ServerInitFinish() {
	//	server_init_finish_ = true;
	//	VLOG(0) << "server_init_finish_ = true !";
	//}

	//void TableGroup::ClearServerFinish() {
	//	server_init_finish_ = false;
	//}

	void TableGroup::WaitServer(int32_t client_iter, int32_t staleness) {
		if (client_iter == 0) {
			BgWorkers::WaitServer(client_iter, 0);
		}
		else {
			BgWorkers::WaitServer(client_iter, staleness);
		}
	}
}
