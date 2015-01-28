#pragma once

#include <pthread.h>
#include <map>
#include <vector>
#include <condition_variable>

#include "system/configs.hpp"
#include "system/ps_msgs.hpp"
#include "system/system_context.hpp"
#include "util/comm_bus.hpp"
#include "util/vector_clock.hpp"
#include "util/mt_queue_move.h"

namespace petuum {

// Relies on GlobalContext being properly initalized.
	//class ServerPushOpLogIterationMsg;
	//class ClientModelSliceRequestMsg;

class BgWorkers {
public:
  static void Init();

  static void Init(util::MtQueueMove<std::unique_ptr<ServerPushOpLogIterationMsg>> *server_delta_queue);

  static void ShutDown();

  static void ThreadRegister();
  static void ThreadDeregister();

  static void RequestModelSlice(int32_t table_id, int32_t slice_id);
  static void CreateSendModelSliceRequestToServer(ClientModelSliceRequestMsg& client_model_slice_request_msg);

  static int32_t GetSystemClock();
  static void WaitSystemClock(int32_t my_clock);
  // Note(v-feigao): Function for SSP in LightLDA
  static void WaitServer(int32_t client_iter, int32_t staleness);
 
private:
  /* Functions that differentiate SSP, SSPPush and SSPPushValue */
  static void *SSPBgThreadMain(void *thread_id);

  /* Helper functions*/
  /* Communication functions */
  static void ConnectToNameNodeOrServer(int32_t server_id);
  static void ConnectToBg(int32_t bg_id);
  static void SendToAllLocalBgThreads(void *msg, int32_t size);
  static void BgServerHandshake();
  static void ShutDownClean();

  static std::vector<pthread_t> threads_;
  static std::vector<int32_t> thread_ids_;

private:
  static util::MtQueueMove<std::unique_ptr<ServerPushOpLogIterationMsg>> *server_delta_queue_;

  static int32_t id_st_;

  static pthread_barrier_t init_barrier_;

  static CommBus *comm_bus_;

  static CommBus::RecvFunc CommBusRecvAny;
  static CommBus::RecvTimeOutFunc CommBusRecvTimeOutAny;
  static CommBus::SendFunc CommBusSendAny;
  static CommBus::RecvAsyncFunc CommBusRecvAsyncAny;
  static CommBus::RecvWrapperFunc CommBusRecvAnyWrapper;

  static void CommBusRecvAnyBusy(int32_t *sender_id, zmq::message_t *zmq_msg);
  static void CommBusRecvAnySleep(int32_t *sender_id, zmq::message_t *zmq_msg);

  static std::mutex system_clock_mtx_;
  static std::condition_variable system_clock_cv_;

  static std::atomic_int_fast32_t system_clock_;
  static VectorClockMT bg_server_clock_;
  static VectorClock app_vector_clock_;

  // Note(v-feigao): Variables for SSP in LightLDA
  static std::mutex server_iter_mtx_;
  static std::condition_variable server_iter_cv_;
  static std::atomic<int> server_iter_;
  static VectorClock server_init_clock_;
};

}  // namespace petuum
