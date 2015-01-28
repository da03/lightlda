// bg_workers.cpp
// author: jinliang
// Modified by : Gao Fei

#include "system/bg_workers.hpp"
#include <utility>
#include "system/ps_msgs.hpp"
#include "util/serialized_row_reader.hpp"
#include "util/stats.hpp"
#include "system/mem_transfer.hpp"

namespace petuum {

	std::vector<pthread_t> BgWorkers::threads_;
	std::vector<int32_t> BgWorkers::thread_ids_;
	util::MtQueueMove<std::unique_ptr<ServerPushOpLogIterationMsg>>* BgWorkers::server_delta_queue_;
	int32_t BgWorkers::id_st_;
	pthread_barrier_t BgWorkers::init_barrier_;
	CommBus *BgWorkers::comm_bus_;

	CommBus::RecvFunc BgWorkers::CommBusRecvAny;
	CommBus::RecvTimeOutFunc BgWorkers::CommBusRecvTimeOutAny;
	CommBus::SendFunc BgWorkers::CommBusSendAny;
	CommBus::RecvAsyncFunc BgWorkers::CommBusRecvAsyncAny;
	CommBus::RecvWrapperFunc BgWorkers::CommBusRecvAnyWrapper;

	std::mutex BgWorkers::system_clock_mtx_;
	std::condition_variable BgWorkers::system_clock_cv_;
	std::atomic_int_fast32_t BgWorkers::system_clock_;
	VectorClockMT BgWorkers::bg_server_clock_;
	VectorClock BgWorkers::app_vector_clock_;

	std::mutex BgWorkers::server_iter_mtx_;
	std::condition_variable BgWorkers::server_iter_cv_;
	std::atomic<int> BgWorkers::server_iter_;
	VectorClock BgWorkers::server_init_clock_;

	void BgWorkers::Init() {
		threads_.resize(GlobalContext::get_num_bg_threads());
		thread_ids_.resize(GlobalContext::get_num_bg_threads());
		id_st_ = GlobalContext::get_head_bg_id(GlobalContext::get_client_id());
		comm_bus_ = GlobalContext::comm_bus;

		int32_t my_client_id = GlobalContext::get_client_id();
		int32_t my_head_bg_id = GlobalContext::get_head_bg_id(my_client_id);
		for (int32_t i = 0; i < GlobalContext::get_num_bg_threads(); ++i) {
			bg_server_clock_.AddClock(my_head_bg_id + i, 0);
		}

		// NOTE(v-feigao): init for app_vector_clock
		for (int32_t i = 1; i < GlobalContext::get_num_app_threads(); ++i) {
			app_vector_clock_.AddClock(i);
		}

		for (int32_t i = 0; i < GlobalContext::get_num_clients(); ++i) {
			server_init_clock_.AddClock(i);
		}

		pthread_barrier_init(&init_barrier_, NULL,
			GlobalContext::get_num_bg_threads() + 1);

		if (GlobalContext::get_num_clients() == 1) {
			CommBusRecvAny = &CommBus::RecvInProc;
			CommBusRecvAsyncAny = &CommBus::RecvInProcAsync;
		}
		else{
			CommBusRecvAny = &CommBus::Recv;
			CommBusRecvAsyncAny = &CommBus::RecvAsync;
		}

		if (GlobalContext::get_num_clients() == 1) {
			CommBusRecvTimeOutAny = &CommBus::RecvInProcTimeOut;
		}
		else{
			CommBusRecvTimeOutAny = &CommBus::RecvTimeOut;
		}

		if (GlobalContext::get_num_clients() == 1) {
			CommBusSendAny = &CommBus::SendInProc;
		}
		else {
			CommBusSendAny = &CommBus::Send;
		}
		if (GlobalContext::get_aggressive_cpu()) {
			CommBusRecvAnyWrapper = CommBusRecvAnyBusy;
		}
		else {
			CommBusRecvAnyWrapper = CommBusRecvAnySleep;
		}

		server_iter_ = -1;

		int i;
		for (i = 0; i < GlobalContext::get_num_bg_threads(); ++i){
			thread_ids_[i] = id_st_ + i;
			int ret = pthread_create(&threads_[i], NULL, SSPBgThreadMain,
				&thread_ids_[i]);
			CHECK_EQ(ret, 0);
		}
		pthread_barrier_wait(&init_barrier_);

		ThreadRegister();
	}

	void BgWorkers::Init(util::MtQueueMove<std::unique_ptr<ServerPushOpLogIterationMsg>> *server_delta_queue)
	{
		threads_.resize(GlobalContext::get_num_bg_threads());
		thread_ids_.resize(GlobalContext::get_num_bg_threads());
		server_delta_queue_ = server_delta_queue;
		id_st_ = GlobalContext::get_head_bg_id(GlobalContext::get_client_id());
		comm_bus_ = GlobalContext::comm_bus;

		int32_t my_client_id = GlobalContext::get_client_id();
		int32_t my_head_bg_id = GlobalContext::get_head_bg_id(my_client_id);
		for (int32_t i = 0; i < GlobalContext::get_num_bg_threads(); ++i) {
			bg_server_clock_.AddClock(my_head_bg_id + i, 0);
		}

		// NOTE(v-feigao): init for app_vector_clock
		for (int32_t i = 1; i < GlobalContext::get_num_app_threads(); ++i) {
			app_vector_clock_.AddClock(i);
		}

		for (int32_t i = 0; i < GlobalContext::get_num_clients(); ++i) {
			server_init_clock_.AddClock(i);
		}

		pthread_barrier_init(&init_barrier_, NULL,
			GlobalContext::get_num_bg_threads() + 1);

		if (GlobalContext::get_num_clients() == 1) {
			CommBusRecvAny = &CommBus::RecvInProc;
			CommBusRecvAsyncAny = &CommBus::RecvInProcAsync;
		}
		else{
			CommBusRecvAny = &CommBus::Recv;
			CommBusRecvAsyncAny = &CommBus::RecvAsync;
		}

		if (GlobalContext::get_num_clients() == 1) {
			CommBusRecvTimeOutAny = &CommBus::RecvInProcTimeOut;
		}
		else{
			CommBusRecvTimeOutAny = &CommBus::RecvTimeOut;
		}

		if (GlobalContext::get_num_clients() == 1) {
			CommBusSendAny = &CommBus::SendInProc;
		}
		else {
			CommBusSendAny = &CommBus::Send;
		}

		if (GlobalContext::get_aggressive_cpu()) {
			CommBusRecvAnyWrapper = CommBusRecvAnyBusy;
		}
		else {
			CommBusRecvAnyWrapper = CommBusRecvAnySleep;
		}

		server_iter_ = -1;

		int i;
		for (i = 0; i < GlobalContext::get_num_bg_threads(); ++i){
			thread_ids_[i] = id_st_ + i;
			int ret = pthread_create(&threads_[i], NULL, SSPBgThreadMain,
				&thread_ids_[i]);
			CHECK_EQ(ret, 0);
		}
		pthread_barrier_wait(&init_barrier_);

		ThreadRegister();
	}

	void BgWorkers::ShutDown(){
		for (int i = 0; i < GlobalContext::get_num_bg_threads(); ++i){
			int ret = pthread_join(threads_[i], NULL);
			CHECK_EQ(ret, 0);
		}
	}

	void BgWorkers::ThreadRegister(){
		int i;
		for (i = 0; i < GlobalContext::get_num_bg_threads(); ++i) {
			int32_t bg_id = thread_ids_[i];
			ConnectToBg(bg_id);
		}
	}

	void BgWorkers::ThreadDeregister(){
		AppThreadDeregMsg msg;
		SendToAllLocalBgThreads(msg.get_mem(), msg.get_size());
	}

	int32_t BgWorkers::GetSystemClock() {
		return static_cast<int32_t>(system_clock_.load());
	}
	void BgWorkers::WaitSystemClock(int32_t my_clock) {
		std::unique_lock<std::mutex> lock(system_clock_mtx_);
		// The bg threads might have advanced the clock after my last check.
		while (static_cast<int32_t>(system_clock_.load()) < my_clock) {
			VLOG(0) << "Wait " << my_clock;
			system_clock_cv_.wait(lock);
			VLOG(0) << "wake up";
		}
	}

	void BgWorkers::WaitServer(int32_t client_iter, int32_t staleness) {
		std::unique_lock<std::mutex> lock(server_iter_mtx_);
		while (client_iter > server_iter_ + staleness) {
			server_iter_cv_.wait(lock);
		}
	}

	/* Private Functions */

	void BgWorkers::ConnectToNameNodeOrServer(int32_t server_id){
		VLOG(0) << "ConnectToNameNodeOrServer server_id = " << server_id;
		ClientConnectMsg client_connect_msg;
		client_connect_msg.get_client_id() = GlobalContext::get_client_id();
		void *msg = client_connect_msg.get_mem();
		int32_t msg_size = client_connect_msg.get_size();

		if (comm_bus_->IsLocalEntity(server_id)) {
			VLOG(0) << "Connect to local server " << server_id;
			comm_bus_->ConnectTo(server_id, msg, msg_size);
		}
		else {
			VLOG(0) << "Connect to remote server " << server_id;
			HostInfo server_info = GlobalContext::get_host_info(server_id);
			std::string server_addr = server_info.ip + ":" + server_info.port;
			VLOG(0) << "server_addr = " << server_addr;
			comm_bus_->ConnectTo(server_id, server_addr, msg, msg_size);
		}
	}

	void BgWorkers::ConnectToBg(int32_t bg_id){
		AppConnectMsg app_connect_msg;
		void *msg = app_connect_msg.get_mem();
		int32_t msg_size = app_connect_msg.get_size();
		comm_bus_->ConnectTo(bg_id, msg, msg_size);
	}

	void BgWorkers::SendToAllLocalBgThreads(void *msg, int32_t size){
		int i;
		for (i = 0; i < GlobalContext::get_num_bg_threads(); ++i){
			int32_t sent_size = comm_bus_->SendInProc(thread_ids_[i], msg, size);
			CHECK_EQ(sent_size, size);
		}
	}

	void BgWorkers::BgServerHandshake(){
		{
			// connect to name node
			int32_t name_node_id = GlobalContext::get_name_node_id();
			ConnectToNameNodeOrServer(name_node_id);

			// wait for ConnectServerMsg
			zmq::message_t zmq_msg;
			int32_t sender_id;
			if (comm_bus_->IsLocalEntity(name_node_id)) {
				comm_bus_->RecvInProc(&sender_id, &zmq_msg);
			}
			else{
				comm_bus_->RecvInterProc(&sender_id, &zmq_msg);
			}
			MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
			CHECK_EQ(sender_id, name_node_id);
			CHECK_EQ(msg_type, kConnectServer) << "sender_id = " << sender_id;
		}

		// connect to servers
		{
		int32_t num_servers = GlobalContext::get_num_servers();
		std::vector<int32_t> server_ids = GlobalContext::get_server_ids();
		CHECK_EQ((size_t)num_servers, server_ids.size());
		for (int i = 0; i < num_servers; ++i){
			int32_t server_id = server_ids[i];
			VLOG(0) << "Connect to server " << server_id;
			ConnectToNameNodeOrServer(server_id);
		}
	}

		// get messages from servers for permission to start
		{
			int32_t num_started_servers = 0;
			for (num_started_servers = 0;
				// receive from all servers and name node
				num_started_servers < GlobalContext::get_num_servers() + 1;
			++num_started_servers){
				zmq::message_t zmq_msg;
				int32_t sender_id;
				(comm_bus_->*CommBusRecvAny)(&sender_id, &zmq_msg);
				MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
				// TODO: in pushing mode, it may receive other types of message
				// from server
				CHECK_EQ(msg_type, kClientStart);
				VLOG(0) << "get kClientStart from " << sender_id;
			}
		}
	}

	//Note(v-feigao): add ModelSliceRequest Handler
	void BgWorkers::CreateSendModelSliceRequestToServer(
		ClientModelSliceRequestMsg& client_model_slice_request_msg) {

		client_model_slice_request_msg.get_client_id() = GlobalContext::get_client_id();
		VLOG(0) << "BgWorkers start send WordTopicTable Request Msg to All Servers";

		for (auto server_id : GlobalContext::get_server_ids()) {
			//client_model_slice_request_msg.get_server_id() = server_id;
			VLOG(0) << "Bgworkers send msg to server : " << server_id;
			//size_t sent_size = (comm_bus_->*CommBusSendAny)(server_id,
			size_t sent_size = comm_bus_->Send(server_id,
				client_model_slice_request_msg.get_mem(),
				client_model_slice_request_msg.get_size());
			CHECK_EQ(sent_size, client_model_slice_request_msg.get_size()) << "Send size error";
		}
		VLOG(0) << "BgWorkers Send WordTopicTable Request Msg to All Servers";
	}

	void BgWorkers::ShutDownClean() {
		FINALIZE_STATS();
	}


	void BgWorkers::CommBusRecvAnyBusy(int32_t *sender_id,
		zmq::message_t *zmq_msg) {
		bool received = (comm_bus_->*CommBusRecvAsyncAny)(sender_id, zmq_msg);
		while (!received) {
			received = (comm_bus_->*CommBusRecvAsyncAny)(sender_id, zmq_msg);
		}
	}

	void BgWorkers::CommBusRecvAnySleep(int32_t *sender_id,
		zmq::message_t *zmq_msg) {
		(comm_bus_->*CommBusRecvAny)(sender_id, zmq_msg);
	}
	// Bg thread initialization logic:
	// I. Establish connections with all server threads (app threads cannot send
	// message to bg threads until this is done);
	// II. Wait on a "Start" message from each server thread;
	// III. Receive connections from all app threads. Server message (currently none
	// for pull model) may come in at the same time.

	void *BgWorkers::SSPBgThreadMain(void *thread_id) {
		//long long maskLL = 0;
		//maskLL |= (1LL << 23);
		//DWORD_PTR mask = maskLL;
		//SetThreadAffinityMask(GetCurrentThread(), mask);
		int32_t my_id = *(reinterpret_cast<int32_t*>(thread_id));

		LOG(INFO) << "Bg Worker starts here, my_id = " << my_id;

		ThreadContext::RegisterThread(my_id);
		REGISTER_THREAD_FOR_STATS(false);

		int32_t num_connected_app_threads = 0;
		int32_t num_deregistered_app_threads = 0;
		int32_t num_shutdown_acked_servers = 0;

		{
			CommBus::Config comm_config;
			comm_config.entity_id_ = my_id;
			comm_config.ltype_ = CommBus::kInProc;
			comm_bus_->ThreadRegister(comm_config);
		}

		// server handshake
		BgServerHandshake();

		pthread_barrier_wait(&init_barrier_);

		// get connection from init thread
		{
			zmq::message_t zmq_msg;
			int32_t sender_id;
			comm_bus_->RecvInProc(&sender_id, &zmq_msg);
			MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
			CHECK_EQ(msg_type, kAppConnect) << "send_id = " << sender_id;
			++num_connected_app_threads;
			// NOTE(jiyuan): we use num_table_threads to indicate the expected number of connections
			// CHECK(num_connected_app_threads <= GlobalContext::get_num_app_threads());
			CHECK(num_connected_app_threads <= GlobalContext::get_num_table_threads());
			VLOG(0) << "get connected from init thread " << sender_id;
		}

		zmq::message_t zmq_msg;
		int32_t sender_id;
		MsgType msg_type;
		void *msg_mem;
		bool destroy_mem = false;
		while (1) {
			CommBusRecvAnyWrapper(&sender_id, &zmq_msg);

			msg_type = MsgBase::get_msg_type(zmq_msg.data());
			destroy_mem = false;

			if (msg_type == kMemTransfer) {
				//VLOG(0) << "Received kMemTransfer message from " << sender_id;
				MemTransferMsg mem_transfer_msg(zmq_msg.data());
				msg_mem = mem_transfer_msg.get_mem_ptr();
				msg_type = MsgBase::get_msg_type(msg_mem);
				destroy_mem = true;
			}
			else {
				msg_mem = zmq_msg.data();
			}

			//VLOG(0) << "msg_type = " << msg_type;
			switch (msg_type) {
			case kAppConnect:
			{
								++num_connected_app_threads;
								/*
								 CHECK(num_connected_app_threads <= GlobalContext::get_num_app_threads())
								 << "num_connected_app_threads = " << num_connected_app_threads
								 << " get_num_app_threads() = "
								 << GlobalContext::get_num_app_threads();
								 */
								// NOTE(jiyuan): expect the num_io_threads() connections rather than 
								// the num_app_threads(), the num_connected_app_threads may be larger than
								// num_io_threads(), since the init thread has connected bg_worker
								
								CHECK(num_connected_app_threads <= GlobalContext::get_num_io_threads() + 1)
									<< "num_connected_table_threads = " << num_connected_app_threads
									<< " get_num_io_threads() = "
									<< GlobalContext::get_num_io_threads() + 1;
			}
				break;
			case kAppThreadDereg:
			{
									++num_deregistered_app_threads;

									// NOTE(jiyuan): expect the num_table_threads() deregistration rather than 
									// the num_app_threads()

									// if (num_deregistered_app_threads == GlobalContext::get_num_app_threads()) {
									if (num_deregistered_app_threads == GlobalContext::get_num_io_threads()) {
										ClientShutDownMsg msg;
										int32_t name_node_id = GlobalContext::get_name_node_id();
										(comm_bus_->*CommBusSendAny)(name_node_id, msg.get_mem(),
											msg.get_size());
										int32_t num_servers = GlobalContext::get_num_servers();
										std::vector<int32_t> &server_ids = GlobalContext::get_server_ids();
										for (int i = 0; i < num_servers; ++i) {
											int32_t server_id = server_ids[i];
											(comm_bus_->*CommBusSendAny)(server_id, msg.get_mem(),
												msg.get_size());
										}
									}
			}
				break;
			case kServerShutDownAck:
			{
									   ++num_shutdown_acked_servers;
									   VLOG(0) << "get ServerShutDownAck from server " << sender_id;
									   if (num_shutdown_acked_servers
										   == GlobalContext::get_num_servers() + 1) {
										   VLOG(0) << "Bg worker " << my_id << " shutting down";
										   comm_bus_->ThreadDeregister();
										   ShutDownClean();
										   return 0;
									   }
			}
				break;
			case kServerUpdateClock:
			{
									   VLOG(0) << "BgWorkers receive Server Update Clock Msg";
									   ServerUpdateClockMsg server_update_clock_msg(msg_mem);
									   int32_t server_id = server_update_clock_msg.get_server_id();
									   int32_t new_clock = server_init_clock_.Tick(server_id);
									   if (new_clock) {
										   //TableGroup
										   server_iter_ += 1;
										   std::unique_lock<std::mutex> lock(server_iter_mtx_);
										   server_iter_cv_.notify_one();
										   VLOG(0) << "Server Init Finished";
										   // TableGroup::ServerInitFinish();
									   }
									   break;
			}
				// the DeltaIOThread send message to bg_worker,
				// Just forward msg to Server.
			case kClientSendOpLogIteration:
			{
											  ClientSendOpLogIterationMsg client_send_oplog_msg(msg_mem);
											  int32_t server_id = client_send_oplog_msg.get_server_id();
											  int32_t app_thread_id = client_send_oplog_msg.get_app_thread_id();
											  CHECK(client_send_oplog_msg.get_table_id() == 1 || client_send_oplog_msg.get_table_id() == 2);
											  size_t sent_size = comm_bus_->Send(server_id, client_send_oplog_msg.get_mem(), client_send_oplog_msg.get_size());
											  CHECK_EQ(sent_size, client_send_oplog_msg.get_size());
			}
				break;

				//NOTE(v-feigao): add handler of ClientModelSliceRequest Msg
				// Note(v-feigao): just forward the message to server. 
			case kClientModelSliceRequest:
			{
											 ClientModelSliceRequestMsg client_model_slice_request_msg(msg_mem);
											 CreateSendModelSliceRequestToServer(client_model_slice_request_msg);
			}
				break;
		
			case kServerPushOpLogIteration:
			{
											  // forward the message to ModelIOThread by insert the message to a queue

											  // server_push_oplog_iteration_msg does not own the memory
											  ServerPushOpLogIterationMsg server_push_oplog_iteration_msg(msg_mem);
											  //size_t msg_size = server_push_oplog_iteration_msg.get_size();

											  // get the available size of the arbitrary message
											  size_t avai_size = server_push_oplog_iteration_msg.get_avai_size();

											  // create a new msg with the same size as server_push_oplog_iteration_msg
											  std::unique_ptr<ServerPushOpLogIterationMsg> msg_ptr(new ServerPushOpLogIterationMsg(avai_size));

											  // copy the server_push_oplog_iteration_msg to new msg
											  msg_ptr->get_table_id() = server_push_oplog_iteration_msg.get_table_id();
											  msg_ptr->get_is_clock() = server_push_oplog_iteration_msg.get_is_clock();
											  msg_ptr->get_server_id() = server_push_oplog_iteration_msg.get_server_id();
											  msg_ptr->get_iteration() = server_push_oplog_iteration_msg.get_iteration();
											  memcpy(msg_ptr->get_data(), server_push_oplog_iteration_msg.get_data(), avai_size);
											//  VLOG(0) << "Bg Workers: reveive from server push msg. iteration: " << msg_ptr->get_iteration()
												//  << " Is_clock: " << msg_ptr->get_is_clock() << " Table id : " << msg_ptr->get_table_id();
											  // move the msg_ptr into the queue
											  // ORIG: server_delta_queue_->Push(std::move(msg_ptr));
											  server_delta_queue_->Push((msg_ptr));


			}
				break;
			default:
				LOG(FATAL) << "Unrecognized type " << msg_type;
			}

			if (destroy_mem)
				MemTransfer::DestroyTransferredMem(msg_mem);
		}

		return 0;
	}

}  // namespace petuum
