/*
 * table_group.hpp
 * author: jinliang
 */

#pragma once

#include <cstdint>
#include <map>
#include <pthread.h>
#include "system/ps_msgs.hpp"
#include "system/configs.hpp"
#include "system/abstract_row.hpp"
#include "util/mt_queue_move.h"
#include "util/vector_clock_mt.hpp"

namespace petuum {
	class TableGroup {
	public:
		// Can be called only once per process. Must be called after RegisterRow() to
		// be a barrier for CreateTable(), and "happen before" any other call to
		// TableGroup. The thread that calls Init() is refered to as the init
		// thread.  If the init thread needs to access table API (e.g., init thread
		// itself being a worker thread), it should set table_access to true. Init
		// thread is responsible for calling RegisterRow(), CreateTable() and
		// ShutDown(). Calling those functions from other threads is not allowed.
		// Init thread does not need to DeregisterThread() nor RegisterThread().
		static int Init(const TableGroupConfig &table_group_config,
			bool table_access);

		// Init thread need to call ShutDown() after all other app threads have
		// deregistered. Any other call to TableGroup and Table API must return
		// before calling ShutDown().
		static void ShutDown();


		static bool CreateTable(int32_t table_id,
			const ClientTableConfig& table_config);

		// Must be called by Init thread after creating all tables and before any
		// other thread calls RegisterThread().
		static void CreateTableDone();

		// Called by Init thread only before it access any table API.
		// Must be called after CreateTableDone().
		// If Init thread does not access table API, it makes no difference calling
		// this function.
		static void WaitThreadRegister();

		static util::MtQueueMove<std::unique_ptr<ServerPushOpLogIterationMsg>>* GetServerDeltaQueue()
		{
			return &server_delta_queue_;
		}


		// A app threads except init thread should register itself before accessing
		// any Table API. In SSP mode, if a thread invokes RegisterThread with
		// true, its clock will be kept track of, so it should call Clock()
		// properly.
		static int32_t RegisterThread();

		// A registered thread must deregister itself.
		static void DeregisterThread();

		static void RequestModelSlice(int32_t table_id, int32_t slice_id);


		// Note(v-feigao): naive implementation for BSP
		//static bool IsServerInitFinish();
		//static void ServerInitFinish();
		//static void ClearServerFinish();

		// Note(v-feigao): For SSP
		static void WaitServer(int32_t client_iter, int32_t staleness);


	private:
		static pthread_barrier_t register_barrier_;
		static std::atomic<int> num_app_threads_registered_;

		// NOTE(jiyuan): for communication between bg_worker and ModelIOThread

		static util::MtQueueMove<std::unique_ptr<ServerPushOpLogIterationMsg>> server_delta_queue_;
		static VectorClockMT app_vector_clock_;
		static bool server_init_finish_;

		static VectorClockMT vector_clock_;
	};

}   // namespace petuum
