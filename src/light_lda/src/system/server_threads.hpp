// server_threads.hpp
// author: jinliang

#pragma once

#include <vector>
#include <pthread.h>
#include <boost/thread/tss.hpp>
#include <queue>

#include "memory/summary_row.hpp"
#include "memory/model_block.h"
#include "system/system_context.hpp"
#include "util/comm_bus.hpp"
#include "util/mt_queue_move.h"
#include "util/sparse_row.hpp"
#include "util/delta_table.h"
#include "util/hybrid_map.h"

namespace petuum {
	class ServerPushOpLogIterationMsg;
	class ClientModelSliceRequestMsg;
	class ClientSendOpLogIterationMsg;

	class ServerThreads {
	public:
		static void Init(int32_t id_st);
		static void ShutDown();

	private:
		// server context is specific to the server thread
		struct ServerContext {
			std::vector<int32_t> bg_thread_ids_;
			int32_t num_shutdown_bgs_;
		};

		static void *ServerThreadMain(void *server_thread_info);
		static void *AggregatorThreadMain(void *server_thread_info);

		// communication function
		// assuming the caller is not name node
		static void ConnectToNameNode();
		static int32_t GetConnection(bool *is_client, int32_t *client_id);

		/**
		 * Functions that operate on the particular thread's specific ServerContext.
		 */
		static void SetUpServerContext();
		static void SetUpCommBus();
		static void SetUpCommBufForAggregator();
		static void InitServer();

		static void SendToAllBgThreads(void *msg, size_t msg_size);
		static bool HandleShutDownMsg(); // returns true if the server may shut down

		static void ServerPushModelSliceMsg(int32_t recv_id,
			ServerPushOpLogIterationMsg* msg, bool is_last);
		static void HandelModelSliceRequest(int32_t sender_id,
			ClientModelSliceRequestMsg &model_slice_request_msg);
		static void SendServerUpdateClockMsg(int32_t iteration);
		static pthread_barrier_t init_barrier;
		// NOTE(jiyuan): sync server thread and aggregator thread
		static pthread_barrier_t aggregator_barrier;
		static std::vector<pthread_t> threads_;
		static std::vector<int32_t> thread_ids_;
		static boost::thread_specific_ptr<ServerContext> server_context_;

		// NOTE(jiyuan): for communication between bg_worker and ModelIOThread
		static util::MtQueueMove<std::unique_ptr<ClientSendOpLogIterationMsg>> client_delta_queue_;
		static VectorClock client_clocks_;
		static pthread_t aggregator_thread_;

		static std::unique_ptr<LDAModelBlock> word_topic_table_;
		static std::unique_ptr<ServerSummaryRow> summary_row_;

		static void DeltaPushSendMsg(int32_t recv_id, ServerPushOpLogIterationMsg* msg, bool is_last);

		static void ApplyOpLog(std::unique_ptr<ClientSendOpLogIterationMsg> &msg_ptr);
		static void ApplyRowOpLog(int iteration, int32_t table_id, int32_t row_id, const void *data, size_t row_size);

		static CommBus::RecvFunc CommBusRecvAny;
		static CommBus::RecvTimeOutFunc CommBusRecvTimeOutAny;
		static CommBus::SendFunc CommBusSendAny;
		static CommBus::RecvAsyncFunc CommBusRecvAsyncAny;
		static CommBus::RecvWrapperFunc CommBusRecvAnyWrapper;

		static void CommBusRecvAnyBusy(int32_t *sender_id, zmq::message_t *zmq_msg);
		static void CommBusRecvAnySleep(int32_t *sender_id, zmq::message_t *zmq_msg);

		static CommBus *comm_bus_;
	};
}
