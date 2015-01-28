// server_thread.cpp
// author: jinliang


#include "server_threads.hpp"
#include "system/system_context.hpp"
#include "system/ps_msgs.hpp"
#include "system/mem_transfer.hpp"
#include "util/stats.hpp"
#include "util/high_resolution_timer.hpp"
#include "util/serialized_row_reader.hpp"


namespace petuum {

	pthread_barrier_t ServerThreads::init_barrier;
	pthread_barrier_t ServerThreads::aggregator_barrier;
	std::vector<pthread_t> ServerThreads::threads_;
	std::vector<int32_t> ServerThreads::thread_ids_;
	boost::thread_specific_ptr<ServerThreads::ServerContext>
		ServerThreads::server_context_;
	CommBus::RecvFunc ServerThreads::CommBusRecvAny;
	CommBus::RecvTimeOutFunc ServerThreads::CommBusRecvTimeOutAny;
	CommBus::SendFunc ServerThreads::CommBusSendAny;
	CommBus::RecvAsyncFunc ServerThreads::CommBusRecvAsyncAny;
	CommBus *ServerThreads::comm_bus_;
	CommBus::RecvWrapperFunc ServerThreads::CommBusRecvAnyWrapper;

	util::MtQueueMove<std::unique_ptr<ClientSendOpLogIterationMsg>> ServerThreads::client_delta_queue_;
	VectorClock ServerThreads::client_clocks_;
	pthread_t ServerThreads::aggregator_thread_;

	std::unique_ptr<LDAModelBlock> ServerThreads::word_topic_table_;
	std::unique_ptr<ServerSummaryRow> ServerThreads::summary_row_;

	void ServerThreads::Init(int32_t id_st){

		// NOTE(jiyuan): local_server_threads = 1, init thread = 1, aggregator thread = 1
		pthread_barrier_init(&init_barrier, NULL,
			GlobalContext::get_num_local_server_threads() + 2);

		// NOTE(jiyuan): used for sync for server thread and aggregator thread
		pthread_barrier_init(&aggregator_barrier, NULL, 2);

		threads_.resize(GlobalContext::get_num_local_server_threads());
		thread_ids_.resize(GlobalContext::get_num_local_server_threads());
		comm_bus_ = GlobalContext::comm_bus;

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

		//TODO(v-feigao): Init the model in parameter server, substitute with LDAModelBlock
		word_topic_table_.reset(new LDAModelBlock(petuum::GlobalContext::get_client_id()));
		// Load model block from the disk
		word_topic_table_->Read(petuum::GlobalContext::get_meta_name());

		if (GlobalContext::get_client_id() == 0)
		{
			summary_row_.reset(new ServerSummaryRow(
				GlobalContext::kSummaryRowID, GlobalContext::get_num_topics()));
		}

		int i;
		/*
		for (i = 0; i < GlobalContext::get_num_local_server_threads(); ++i) {
		VLOG(0) << "Create server thread " << i;
		thread_ids_[i] = id_st + i;
		int ret = pthread_create(&threads_[i], NULL, ServerThreadMain,
		&thread_ids_[i]);
		CHECK_EQ(ret, 0);
		}
		*/
		// NOTE(jiyuan): we temporally assume there is only one server thread in each client
		thread_ids_[0] = id_st + 0;
		int ret = pthread_create(&threads_[0], NULL, ServerThreadMain,
			&thread_ids_[0]);
		CHECK_EQ(ret, 0);

		int32_t aggregator_thread_id = id_st + 1;


		//thread_ids_[1] = id_st + 1;
		ret = pthread_create(&aggregator_thread_, NULL, AggregatorThreadMain,
			&aggregator_thread_id);
		CHECK_EQ(ret, 0);

		pthread_barrier_wait(&init_barrier);
	}

	void ServerThreads::ShutDown(){
		for (int i = 0; i < GlobalContext::get_num_local_server_threads(); ++i){
			int ret = pthread_join(threads_[i], NULL);
			CHECK_EQ(ret, 0);
		}
		// note(v-feigao): add queue exit here.
		client_delta_queue_.Exit();
		int ret = pthread_join(aggregator_thread_, NULL);
		CHECK_EQ(ret, 0);
	}

	/* Private Functions */

	void ServerThreads::SendToAllBgThreads(void *msg, size_t msg_size){
		int32_t i;
		for (i = 0; i < GlobalContext::get_num_total_bg_threads(); ++i){
			int32_t bg_id = server_context_->bg_thread_ids_[i];
			size_t sent_size = (comm_bus_->*CommBusSendAny)(bg_id, msg, msg_size);
			CHECK_EQ(sent_size, msg_size);
		}
	}

	void ServerThreads::ConnectToNameNode(){
		int32_t name_node_id = GlobalContext::get_name_node_id();

		ServerConnectMsg server_connect_msg;
		void *msg = server_connect_msg.get_mem();
		int32_t msg_size = server_connect_msg.get_size();

		if (comm_bus_->IsLocalEntity(name_node_id)) {
			VLOG(0) << "Connect to local name node";
			comm_bus_->ConnectTo(name_node_id, msg, msg_size);
		}
		else {
			VLOG(0) << "Connect to remote name node";
			HostInfo name_node_info = GlobalContext::get_host_info(name_node_id);
			std::string name_node_addr = name_node_info.ip + ":" + name_node_info.port;
			VLOG(0) << "name_node_addr = " << name_node_addr;
			comm_bus_->ConnectTo(name_node_id, name_node_addr, msg, msg_size);
		}
	}

	int32_t ServerThreads::GetConnection(bool *is_client, int32_t *client_id){
		int32_t sender_id;
		zmq::message_t zmq_msg;
		(comm_bus_->*CommBusRecvAny)(&sender_id, &zmq_msg);
		MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
		if (msg_type == kClientConnect) {
			ClientConnectMsg msg(zmq_msg.data());
			*is_client = true;
			*client_id = msg.get_client_id();
		}
		else {
			CHECK_EQ(msg_type, kServerConnect);
			*is_client = false;
		}
		return sender_id;
	}

	void ServerThreads::InitServer() {
		VLOG(0) << "Server connect to name node";
		ConnectToNameNode();

		int32_t num_bgs;
		for (num_bgs = 0; num_bgs < GlobalContext::get_num_total_bg_threads();
			++num_bgs){
			int32_t client_id;
			bool is_client;
			int32_t bg_id = GetConnection(&is_client, &client_id);
			CHECK(is_client);
			server_context_->bg_thread_ids_[num_bgs] = bg_id;
			client_clocks_.AddClock(client_id, 0);
		}

		ClientStartMsg client_start_msg;
		int32_t msg_size = client_start_msg.get_size();
		SendToAllBgThreads(client_start_msg.get_mem(), msg_size);

		VLOG(0) << "InitNonNameNode done";
	}

	bool ServerThreads::HandleShutDownMsg(){
		// When num_shutdown_bgs reaches the total number of bg threads, the server
		// reply to each bg with a ShutDownReply message
		int32_t &num_shutdown_bgs = server_context_->num_shutdown_bgs_;
		++num_shutdown_bgs;
		if (num_shutdown_bgs == GlobalContext::get_num_total_bg_threads()){
			ServerShutDownAckMsg shut_down_ack_msg;
			size_t msg_size = shut_down_ack_msg.get_size();
			int i;
			for (i = 0; i < GlobalContext::get_num_total_bg_threads(); ++i){
				int32_t bg_id = server_context_->bg_thread_ids_[i];
				size_t sent_size = (comm_bus_->*CommBusSendAny)(bg_id,
					shut_down_ack_msg.get_mem(), msg_size);
				CHECK_EQ(msg_size, sent_size);
			}
			return true;
		}
		return false;
	}

	void ServerThreads::SetUpServerContext(){
		server_context_.reset(new ServerContext);
		server_context_->bg_thread_ids_.resize(
			GlobalContext::get_num_total_bg_threads());
		server_context_->num_shutdown_bgs_ = 0;
	}

	void ServerThreads::SetUpCommBus() {
		int32_t my_id = ThreadContext::get_id();
		CommBus::Config comm_config;
		comm_config.entity_id_ = my_id;
		VLOG(0) << "ServerThreads num_clients = " << GlobalContext::get_num_clients();
		VLOG(0) << "my id = " << my_id;

		if (GlobalContext::get_num_clients() > 1) {
			comm_config.ltype_ = CommBus::kInProc | CommBus::kInterProc;
			HostInfo host_info = GlobalContext::get_host_info(my_id);
			comm_config.network_addr_ = host_info.ip + ":" + host_info.port;
			VLOG(0) << "network addr = " << comm_config.network_addr_;
		}
		else {
			comm_config.ltype_ = CommBus::kInProc;
		}

		comm_bus_->ThreadRegister(comm_config);
		VLOG(0) << "Server thread registered CommBus";
	}

	void ServerThreads::SetUpCommBufForAggregator()
	{
		int32_t my_id = ThreadContext::get_id();
		CommBus::Config comm_config;
		comm_config.entity_id_ = my_id;
		VLOG(0) << "ServerThreads num_clients = " << GlobalContext::get_num_clients();
		VLOG(0) << "my id = " << my_id;
		comm_config.ltype_ = CommBus::kInProc;

		comm_bus_->ThreadRegister(comm_config);
		VLOG(0) << "Aggregator Server thread registered CommBus";
	}

	// Note(v-feigao): add handler of ClientModelSliceRequest Msg
	void ServerThreads::HandelModelSliceRequest(int32_t sender_id,
		ClientModelSliceRequestMsg &model_slice_request_msg) {
		// Send related meg;

		int32_t client_id = model_slice_request_msg.get_client_id();

		VLOG(0) << "Server send word topic table slice msg to client " << client_id;
		LOG(INFO) << "Server send word topic table slice msg to client " << client_id;
		// Todo(v-feigao): add ServerCreateSendModelSliceMsg in LDAModelBlock
		word_topic_table_->ServerCreateSendModelSliceMsg(client_id, model_slice_request_msg, DeltaPushSendMsg);

		if (0 == GlobalContext::get_client_id()) {
			VLOG(0) << "Server send summary row msg to client " << client_id;
			LOG(INFO) << "Server send summary row msg to client " << client_id;
            //summary_row_->table_id_ = 2;
			summary_row_->ServerCreateSendModelSliceMsg(client_id, DeltaPushSendMsg);
		}
	}


	void ServerThreads::CommBusRecvAnyBusy(int32_t *sender_id,
		zmq::message_t *zmq_msg) {
		bool received = (comm_bus_->*CommBusRecvAsyncAny)(sender_id, zmq_msg);
		while (!received) {
			received = (comm_bus_->*CommBusRecvAsyncAny)(sender_id, zmq_msg);
		}
	}

	void ServerThreads::CommBusRecvAnySleep(int32_t *sender_id,
		zmq::message_t *zmq_msg) {
		(comm_bus_->*CommBusRecvAny)(sender_id, zmq_msg);
	}

	void ServerThreads::DeltaPushSendMsg(int32_t recv_id, ServerPushOpLogIterationMsg* msg, bool is_last) {
		msg->get_is_clock() = is_last;
		msg->get_server_id() = GlobalContext::get_server_ids()[GlobalContext::get_client_id()];// thread_ids_[0];
		size_t sent_size = comm_bus_->Send(recv_id, msg->get_mem(), msg->get_size());
		CHECK_EQ(sent_size, msg->get_size());
	}

	void ServerThreads::SendServerUpdateClockMsg(int32_t iteration) {
		
		VLOG(0) << "Send Server Update Clock msg to ServerThread";
		ServerUpdateClockMsg msg;
		msg.get_iteration() = iteration;
		msg.get_server_id() = GlobalContext::get_client_id();
		int32_t msg_size = msg.get_size();

		int32_t server_id = GlobalContext::get_server_ids()[GlobalContext::get_client_id()];
		size_t sent_size = comm_bus_->Send(server_id, msg.get_mem(), msg.get_size());
		CHECK_EQ(msg_size, sent_size);
		VLOG(0) << "Finish Send Server Update Clock msg to ServerThread";
	}

	void* ServerThreads::AggregatorThreadMain(void *thread_id)
	{
		int32_t my_id = *(reinterpret_cast<int32_t*>(thread_id));
		ThreadContext::RegisterThread(my_id);

		SetUpCommBufForAggregator();

		// connect to server thread
		int32_t server_id = my_id - 1;
		AggregatorConnectMsg aggregator_connect_msg;
		void *msg = aggregator_connect_msg.get_mem();
		aggregator_connect_msg.get_aggregator_id() = my_id;
		int32_t msg_size = aggregator_connect_msg.get_size();

		pthread_barrier_wait(&aggregator_barrier);
		comm_bus_->ConnectTo(server_id, msg, msg_size);
		pthread_barrier_wait(&init_barrier);
		bool init = false;
		int32_t iter = 0;
		std::string dump_file = GlobalContext::get_dump_file();
		int32_t dump_iter = GlobalContext::get_dump_iter();
		while (true)
		{

			std::unique_ptr<petuum::ClientSendOpLogIterationMsg> msg_ptr;
			if (!client_delta_queue_.Pop(msg_ptr))
			{
				break;
			}
			bool is_iteration_clock = msg_ptr->get_is_iteration_clock();
			int32_t client_id = msg_ptr->get_client_id();
			int32_t iteration = msg_ptr->get_iteration();
			if (msg_ptr->get_table_id() == GlobalContext::kWordTopicTableID) {
				VLOG(0) << "Update word topic table";
				// Todo(v-feigao): add ApplyClientSendOplogMsg in LDAModelBlock
				word_topic_table_->ApplyClientSendOpLogIterationMsg(*msg_ptr);
				VLOG(0) << "Updata word topic successfully";
			}
			else if (msg_ptr->get_table_id() == GlobalContext::kSummaryRowID) {
				CHECK_EQ(GlobalContext::get_client_id(), 0);
				VLOG(0) << "Updata summary row";
				summary_row_->ApplyClientSendOpLogIterationMsg(*msg_ptr);
				VLOG(0) << "Updata summary row successfully";
			}
			else {
				LOG(FATAL) << "Invalid table id. table id = " << msg_ptr->get_table_id();
			}

			VLOG(0) << "Server Aggregator apply oplog msg. Iteration = " << iteration
				<< ". Is_iteration_clock = " << is_iteration_clock;

			if (is_iteration_clock) {
				int32_t new_clock = client_clocks_.Tick(client_id);
				if (new_clock) {
					// Send ServerInit
					LOG(INFO) << "Server finish update delta, iter = " << iter << "Send Server Update Clock msg";
					SendServerUpdateClockMsg(iteration);

					if ((iter+1) % dump_iter == 0) {
						LOG(INFO) << "Server WordTopicTable Dump Model";

						word_topic_table_->Dump(dump_file + ".word_topic_table." 
							+ std::to_string(iter) + std::to_string(GlobalContext::get_client_id()));
						if (GlobalContext::get_client_id() == 0) {
							summary_row_->Dump(dump_file + ".summary_row." + std::to_string(iter));
						}
					}
					LOG(INFO) << "Server finish Dump Model. iter = " << iter;
					++iter;
				}
			}
		}
		LOG(INFO) << "Server Aggregator thread exit";
		return 0;
	}

	void *ServerThreads::ServerThreadMain(void *thread_id){
		//long long maskLL = 0;
		//maskLL |= (1LL << 21);
		//DWORD_PTR mask = maskLL;
		//SetThreadAffinityMask(GetCurrentThread(), mask);
		int32_t my_id = *(reinterpret_cast<int32_t*>(thread_id));

		ThreadContext::RegisterThread(my_id);
		REGISTER_THREAD_FOR_STATS(false);

		// set up thread-specific server context
		SetUpServerContext();
		SetUpCommBus();
		pthread_barrier_wait(&aggregator_barrier);
		int32_t sender_id;
		zmq::message_t zmq_msg_aggregator;
		(comm_bus_->*CommBusRecvAny)(&sender_id, &zmq_msg_aggregator);
		MsgType msg_type = MsgBase::get_msg_type(zmq_msg_aggregator.data());
		if (msg_type == kAggregatorConnect) {
			AggregatorConnectMsg msg(zmq_msg_aggregator.data());
			int32_t aggregator_id = msg.get_aggregator_id();
			if (sender_id != my_id + 1 || sender_id != aggregator_id)
			{
				LOG(FATAL) << "Aggregator thread ID should be server thread id + 1";
			}
		}
		else
		{
			LOG(FATAL) << "Server thread expects aggregator thread to connect";
		}

		pthread_barrier_wait(&init_barrier);

		InitServer();

		zmq::message_t zmq_msg;
		//int32_t sender_id;
		//MsgType msg_type;
		void *msg_mem;
		bool destroy_mem = false;
		while (1) {
			CommBusRecvAnyWrapper(&sender_id, &zmq_msg);

			msg_type = MsgBase::get_msg_type(zmq_msg.data());
			//VLOG(0) << "msg_type = " << msg_type;
			destroy_mem = false;

			if (msg_type == kMemTransfer) {
				MemTransferMsg mem_transfer_msg(zmq_msg.data());
				msg_mem = mem_transfer_msg.get_mem_ptr();
				msg_type = MsgBase::get_msg_type(msg_mem);
				destroy_mem = true;
			}
			else {
				msg_mem = zmq_msg.data();
			}

			switch (msg_type)
			{
			case kClientShutDown:
			{
									VLOG(0) << "get ClientShutDown from bg " << sender_id;
									bool shutdown = HandleShutDownMsg();
									if (shutdown) {
										VLOG(0) << "Server shutdown";
										// client_delta_queue_.Exit();
										comm_bus_->ThreadDeregister();
										FINALIZE_STATS();
										return 0;
									}
									break;
			}
		
				// NOTE(v-feigao): add handler of kClientModelSliceRequest msg
			case kClientModelSliceRequest: {
											   VLOG(0) << "Received ModelSliceRequest Msg!";
											   ClientModelSliceRequestMsg model_slice_request_msg(msg_mem);
											   HandelModelSliceRequest(sender_id, model_slice_request_msg);
											   break;
			}

				// NOTE(jiyuan): add handle of the kClientSendOpLogIteration msg
			case kClientSendOpLogIteration:
			{
											  /*
											  VLOG(0) << "Received OpLogIteration Msg!";
											  ClientSendOpLogIterationMsg client_send_oplog_iteration_msg(msg_mem);
											  TIMER_BEGIN(0, SERVER_HANDLE_OPLOG_MSG);
											  HandleOpLogIterationMsg(sender_id, client_send_oplog_iteration_msg);
											  TIMER_END(0, SERVER_HANDLE_OPLOG_MSG);
											  */

											  // forward the message to AggregatorThread by inserting the message to a queue

											  // server_push_oplog_iteration_msg does not own the memory
											  ClientSendOpLogIterationMsg client_send_oplog_iteration_msg(msg_mem);
											  //size_t msg_size = server_push_oplog_iteration_msg.get_size();

											  // get the available size of the arbitrary message
											  size_t avai_size = client_send_oplog_iteration_msg.get_avai_size();

											  // create a new msg with the same size as server_push_oplog_iteration_msg
											  std::unique_ptr<ClientSendOpLogIterationMsg> msg_ptr(new ClientSendOpLogIterationMsg(avai_size));

											  // copy the server_push_oplog_iteration_msg to new msg
											  msg_ptr->get_table_id() = client_send_oplog_iteration_msg.get_table_id();
											  msg_ptr->get_is_clock() = client_send_oplog_iteration_msg.get_is_clock();
											  msg_ptr->get_client_id() = client_send_oplog_iteration_msg.get_client_id();
											  msg_ptr->get_iteration() = client_send_oplog_iteration_msg.get_iteration();
											  msg_ptr->get_is_iteration_clock() = client_send_oplog_iteration_msg.get_is_iteration_clock();
							
											  memcpy(msg_ptr->get_data(), client_send_oplog_iteration_msg.get_data(), avai_size);
											  VLOG(0) << "Server threads receive kClientSendOpLogIteration. iteration: " << msg_ptr->get_iteration()
												  << "is_clock = " << msg_ptr->get_is_clock() << " table id = " << msg_ptr->get_table_id();

											  // move the msg_ptr into the queue
											  // ORIG: client_delta_queue_.Push(std::move(msg_ptr));
											  client_delta_queue_.Push((msg_ptr));

											  break;
			}
				// v-feigao: add Handler of kServerPushOpLogIteration 
				// Aggregator thread transfer this msg to server, server then send it out.
			case kServerPushOpLogIteration:
			{
											  ServerPushOpLogIterationMsg server_push_msg(msg_mem);
											  int32_t bg_id = server_push_msg.get_bg_id();
											  size_t sent_size = comm_bus_->Send(bg_id, server_push_msg.get_mem(), server_push_msg.get_size());
											  CHECK_EQ(sent_size, server_push_msg.get_size());
											  VLOG(0) << "Server Thread send push msg. Iteration: " << server_push_msg.get_iteration();
			}
				break;
			case kServerUpdateClock: {
										 ServerUpdateClockMsg server_update_clock_msg(msg_mem);
										 for (int32_t i = 0; i < GlobalContext::get_num_clients(); ++i) {
											 int32_t bg_id = GlobalContext::get_head_bg_id(i);
											 size_t sent_size = comm_bus_->Send(
												 bg_id, server_update_clock_msg.get_mem(), server_update_clock_msg.get_size());
											 CHECK_EQ(sent_size, server_update_clock_msg.get_size());
											 VLOG(0) << "Server Thread send Init Update Clock msg. ";
										 }
										 break;
			}
			default:
				LOG(FATAL) << "Unrecognized message type " << msg_type;
			}

			if (destroy_mem)
				MemTransfer::DestroyTransferredMem(msg_mem);
		}
	}
}
