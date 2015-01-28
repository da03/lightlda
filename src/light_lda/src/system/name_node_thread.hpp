// namenode_threads.hpp
// author: jinliang

#pragma once

#include <vector>
#include <queue>
#include <pthread.h>
#include <boost/thread/tss.hpp>
#include "util/comm_bus.hpp"

namespace petuum {

class NameNodeThread {
public:
  static void Init();
  static void ShutDown();

private:
  // server context is specific to the server thread
  struct NameNodeContext {
    std::vector<int32_t> bg_thread_ids_;
    int32_t num_shutdown_bgs_;
  };

  static void *NameNodeThreadMain(void *server_thread_info);

  // communication function
  static int32_t GetConnection(bool *is_client, int32_t *client_id);
  static void SendToAllServers(void *msg, size_t msg_size);

  /**
   * Functions that operate on the particular thread's specific NameNodeContext.
   */
  static void SetUpNameNodeContext();
  static void SetUpCommBus();
  static void InitNameNode();

  static void SendToAllBgThreads(void *msg, size_t msg_size);
  static bool HandleShutDownMsg(); // returns true if the server may shut down

  static pthread_barrier_t init_barrier;
  static pthread_t thread_;
  static boost::thread_specific_ptr<NameNodeContext> name_node_context_;

  static CommBus::RecvFunc CommBusRecvAny;
  static CommBus::RecvTimeOutFunc CommBusRecvTimeOutAny;
  static CommBus::SendFunc CommBusSendAny;

  static CommBus *comm_bus_;
};
}
