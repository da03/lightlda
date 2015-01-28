// ps_msgs.hpp
// author: jinliang

#pragma once

// used to decide if a message should be allocated on heap or
// on stack

#define PETUUM_MSG_STACK_BUFF_SIZE 32  // number of bytes

#include "util/mem_block.hpp"

namespace petuum {

enum MsgType {
	kClientConnect = 0,
	kServerConnect = 1,
	kAppConnect = 2,
	//kBgCreateTable = 3,
	//kCreateTable = 4,
	//kCreateTableReply = 5,
	//kCreatedAllTables = 6,
	//kRowRequest = 7,
	//kRowRequestReply = 8,
	//kServerRowRequestReply = 9,
	//kBgClock = 10,
	//kBgSendOpLog = 11,
	//kClientSendOpLog = 12,
	kConnectServer = 13,
	kClientStart = 14,
	kAppThreadDereg = 15,
	kClientShutDown = 16,
	kServerShutDownAck = 17,
	//kServerPushRow = 18,

	kClientSendOpLogIteration = 19,
	kServerPushOpLogIteration = 20,
	kBgSendOpLogIteration = 21,
	kAggregatorConnect = 22,

	kClientModelSliceRequest = 23,
	kServerModelSliceRequestReply = 24,

	kServerUpdateClock = 25,

	kMemTransfer = 50
};

// Represent a message over wire that is very easy to serialize.
// Can either create a message (allocate memory) or take an existing
// chunk of memory and parse it as message. User is responsible for
// creating the correct message over the existing memory.
// Also, if the memory is not allocated via MemBloc::MemAlloc(),
// remember to release it otherwise the deallocation might not work.

// All message types except MsgBase are derived from some parent message
// type while MsgBase is the ancestor of all other message types.

// Normally each message type provides to construstors:
// 1) Default constructor: it checks the desirable size of the message and
// allocate the right amount of memory and let MemBlock take control of that.
// Note that we adapt the rationale of 0MQ messages:
// (http://zeromq.org/docs:message-api-goals)
// For very small messages, it's cheaper to copy the message than keeping it on
// heap. Therefore, very small messages
// (size less than PETUUM_MSG_STACK_BUFF_SIZE), are allocated on stack.
// 2) The second constructor takes in a pre-allocated memory and use it as the
// message buffer.

// A message has a message buffer and uses offset into the buffer to provide
// accesses to different "fields". The parent type's fields are located before
// (lower address) the child type's fields and the size of the message that the
// parent (incuding ancestors) accesses is get_size(). Here are the rules to
// to ensure the messages are constructed and used properly:
// 1) Any message type that may be a parent memory type may not allocate memory
// in the default constructor, because that may lead to memory leak unless the
// child type's contructor always checks to free memory which is expensive;
// 2) Because of 1), any parent message type can only be constructed with a
// pre-allocated message buffer;
// 3) When computing the offset to a field, should always start from the parent
// type's size;
// 4) Same thing as 3) when computing the size of a message.

struct MsgBase : boost::noncopyable {
public:
  MsgBase():
    use_stack_buff_(false) {}

  explicit MsgBase(void *msg):
    own_mem_(false),
    use_stack_buff_(false) {
    mem_.Reset(msg);
  }

  virtual ~MsgBase() {
    if (!own_mem_) {
      mem_.Release();
    }
  }

  uint8_t *get_mem() {
    return mem_.get_mem();
  }

  virtual size_t get_size() {
    return sizeof(MsgType);
  }

  bool get_use_stack_buff() {
    return use_stack_buff_;
  }

  // can be used whether or not mem is owned
  void *ReleaseMem() {
    own_mem_ = false;
    return mem_.Release();
  }

  MsgType &get_msg_type() {
    return *(reinterpret_cast<MsgType*>(mem_.get_mem()));
  }

  static MsgType &get_msg_type(void *mem) {
    return *(reinterpret_cast<MsgType*>(mem));
  }

protected:
  virtual void InitMsg() {}

  MemBlock mem_;
  bool own_mem_;  // if memory is owned by MemBlock

  uint8_t stack_buff_[PETUUM_MSG_STACK_BUFF_SIZE];
  bool use_stack_buff_;
};

struct NumberedMsg : public MsgBase {
public:
  NumberedMsg() {}

  explicit NumberedMsg(void *msg):
    MsgBase(msg) {}

  uint32_t &get_seq_num() {
    return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
    + MsgBase::get_size()));
  }

  uint32_t &get_ack_num() {
    return *(reinterpret_cast<uint32_t*>(mem_.get_mem() + MsgBase::get_size()
      + sizeof(uint32_t)));
  }

  virtual size_t get_size() {
    return (MsgBase::get_size() + sizeof(uint32_t) + sizeof(uint32_t));
  }
};

struct ClientConnectMsg : public NumberedMsg {
public:
  ClientConnectMsg() {
    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
      own_mem_ = true;
      use_stack_buff_ = false;
      mem_.Alloc(get_size());
    } else {
      own_mem_ = false;
      use_stack_buff_ = true;
      mem_.Reset(stack_buff_);
    }
    InitMsg();
  }

  explicit ClientConnectMsg(void *msg):
    NumberedMsg(msg) {}

  int32_t &get_client_id() {
    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
      + NumberedMsg::get_size()));
  }

  size_t get_size() {
    return NumberedMsg::get_size() + sizeof(int32_t);
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kClientConnect;
  }
};

struct AggregatorConnectMsg : public NumberedMsg {
public:
	AggregatorConnectMsg() {
		if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
			own_mem_ = true;
			use_stack_buff_ = false;
			mem_.Alloc(get_size());
		}
		else {
			own_mem_ = false;
			use_stack_buff_ = true;
			mem_.Reset(stack_buff_);
		}
		InitMsg();
	}

	explicit AggregatorConnectMsg(void *msg) :
		NumberedMsg(msg) {}

	int32_t &get_aggregator_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ NumberedMsg::get_size()));
	}

	size_t get_size() {
		return NumberedMsg::get_size() + sizeof(int32_t);
	}

protected:
	void InitMsg() {
		NumberedMsg::InitMsg();
		get_msg_type() = kAggregatorConnect;
	}
};

struct ServerConnectMsg : public NumberedMsg {
public:
  ServerConnectMsg() {
    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
      own_mem_ = true;
      use_stack_buff_ = false;
      mem_.Alloc(get_size());
    } else {
      own_mem_ = false;
      use_stack_buff_ = true;
      mem_.Reset(stack_buff_);
    }
    InitMsg();
  }

  explicit ServerConnectMsg(void *msg):
    NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kServerConnect;
  }
};

struct AppConnectMsg : public NumberedMsg {
public:
  AppConnectMsg() {
    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
      own_mem_ = true;
      use_stack_buff_ = false;
      mem_.Alloc(get_size());
    } else {
      own_mem_ = false;
      use_stack_buff_ = true;
      mem_.Reset(stack_buff_);
    }
    InitMsg();
  }

  explicit AppConnectMsg(void *msg):
    NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kAppConnect;
  }
};
/*
//struct BgCreateTableMsg : public NumberedMsg {
//public:
//  BgCreateTableMsg() {
//    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
//      own_mem_ = true;
//      use_stack_buff_ = false;
//      mem_.Alloc(get_size());
//    } else {
//      own_mem_ = false;
//      use_stack_buff_ = true;
//      mem_.Reset(stack_buff_);
//    }
//    InitMsg();
//  }
//
//  explicit BgCreateTableMsg(void *msg):
//    NumberedMsg(msg) {}
//
//  size_t get_size() {
//    return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
//      + sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(int32_t)
//      + sizeof(int32_t) + sizeof(int32_t);
//  }
//
//  int32_t &get_table_id() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + NumberedMsg::get_size()));
//  }
//
//  int32_t &get_staleness() {
//    return *(reinterpret_cast<int32_t *>(mem_.get_mem()
//      + NumberedMsg::get_size() + sizeof(int32_t)));
//  }
//
//  int32_t &get_row_type() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)));
//  }
//
//  int32_t &get_row_capacity() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//     + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
//     + sizeof(int32_t)));
//  }
//
//  int32_t &get_process_cache_capacity() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//     + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
//     + sizeof(int32_t) + sizeof(int32_t)));
//  }
//
//  int32_t &get_thread_cache_capacity() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//     + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
//     + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t)));
//  }
//
//  int32_t &get_oplog_capacity() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
//      + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t)
//      + sizeof(int32_t)));
//  }
//
//protected:
//  void InitMsg() {
//    NumberedMsg::InitMsg();
//    get_msg_type() = kBgCreateTable;
//  }
//};
//
//struct CreateTableMsg : public NumberedMsg {
//public:
//  CreateTableMsg() {
//    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
//      own_mem_ = true;
//      use_stack_buff_ = false;
//      mem_.Alloc(get_size());
//    } else {
//      own_mem_ = false;
//      use_stack_buff_ = true;
//      mem_.Reset(stack_buff_);
//     }
//     InitMsg();
//  }
//
//  explicit CreateTableMsg(void *msg):
//    NumberedMsg(msg) {}
//
//  size_t get_size() {
//    return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
//      + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t);
//  }
//
//  int32_t &get_table_id() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + NumberedMsg::get_size()));
//  }
//
//  int32_t &get_staleness() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + NumberedMsg::get_size() + sizeof(int32_t)));
//  }
//
//  int32_t &get_row_type() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem() + NumberedMsg::get_size()
//      + sizeof(int32_t) + sizeof(int32_t)));
//  }
//
//  int32_t &get_row_capacity() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem() + NumberedMsg::get_size()
//      + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t)));
//  }
//
//protected:
//  void InitMsg() {
//    NumberedMsg::InitMsg();
//    get_msg_type() = kCreateTable;
//  }
//};
//
//struct CreateTableReplyMsg : public NumberedMsg {
//public:
//  CreateTableReplyMsg() {
//    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
//      own_mem_ = true;
//      use_stack_buff_ = false;
//      mem_.Alloc(get_size());
//     } else {
//      own_mem_ = false;
//      use_stack_buff_ = true;
//      mem_.Reset(stack_buff_);
//    }
//    InitMsg();
//  }
//
//  explicit CreateTableReplyMsg(void *msg):
//    NumberedMsg(msg) {}
//
//  size_t get_size() {
//    return NumberedMsg::get_size() + sizeof(int32_t);
//  }
//
//  int32_t &get_table_id() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + NumberedMsg::get_size()));
//  }
//
//protected:
//  void InitMsg(){
//    NumberedMsg::InitMsg();
//    get_msg_type() = kCreateTableReply;
//  }
//};
//
//struct RowRequestMsg : public NumberedMsg {
//public:
//  RowRequestMsg() {
//    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
//      own_mem_ = true;
//      use_stack_buff_ = false;
//      mem_.Alloc(get_size());
//    } else {
//      own_mem_ = false;
//      use_stack_buff_ = true;
//      mem_.Reset(stack_buff_);
//    }
//    InitMsg();
//  }
//
//  explicit RowRequestMsg(void *msg):
//    NumberedMsg(msg) {}
//
//  size_t get_size() {
//    return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
//      + sizeof(int32_t);
//  }
//
//  int32_t &get_table_id() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + NumberedMsg::get_size()));
//  }
//
//  int32_t &get_row_id() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + NumberedMsg::get_size() + sizeof(int32_t)));
//  }
//
//  int32_t &get_clock() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)));
//  }
//
//protected:
//  void InitMsg() {
//    NumberedMsg::InitMsg();
//    get_msg_type() = kRowRequest;
//  }
//};
//
//struct RowRequestReplyMsg : public NumberedMsg {
//public:
//  RowRequestReplyMsg() {
//    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
//      own_mem_ = true;
//      use_stack_buff_ = false;
//      mem_.Alloc(get_size());
//    } else {
//      own_mem_ = false;
//      use_stack_buff_ = true;
//      mem_.Reset(stack_buff_);
//    }
//    InitMsg();
//  }
//
//  explicit RowRequestReplyMsg(void *msg):
//    NumberedMsg(msg) {}
//
//  size_t get_size() {
//    return NumberedMsg::get_size();
//  }
//
//protected:
//  void InitMsg() {
//    NumberedMsg::InitMsg();
//    get_msg_type() = kRowRequestReply;
//  }
//};
//
//struct CreatedAllTablesMsg : public NumberedMsg {
//public:
//  CreatedAllTablesMsg() {
//     if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
//       own_mem_ = true;
//       use_stack_buff_ = false;
//       mem_.Alloc(get_size());
//     } else {
//       own_mem_ = false;
//       use_stack_buff_ = true;
//       mem_.Reset(stack_buff_);
//     }
//     InitMsg();
//  }
//
//  explicit CreatedAllTablesMsg(void *msg):
//    NumberedMsg(msg) {}
//
//protected:
//  void InitMsg(){
//    NumberedMsg::InitMsg();
//    get_msg_type() = kCreatedAllTables;
//  }
//};
*/

struct ConnectServerMsg : public NumberedMsg {
public:
  ConnectServerMsg() {
    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
      own_mem_ = true;
      use_stack_buff_ = false;
      mem_.Alloc(get_size());
     } else {
      own_mem_ = false;
      use_stack_buff_ = true;
      mem_.Reset(stack_buff_);
    }
    InitMsg();
  }

  explicit ConnectServerMsg(void *msg):
    NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kConnectServer;
  }
};

struct ClientStartMsg : public NumberedMsg {
public:
  ClientStartMsg() {
    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
      own_mem_ = true;
      use_stack_buff_ = false;
      mem_.Alloc(get_size());
    } else {
      own_mem_ = false;
      use_stack_buff_ = true;
      mem_.Reset(stack_buff_);
    }
    InitMsg();
  }

  explicit ClientStartMsg(void *msg):
    NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kClientStart;
  }
};

struct AppThreadDeregMsg : public NumberedMsg {
public:
  AppThreadDeregMsg() {
    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
      own_mem_ = true;
      use_stack_buff_ = false;
      mem_.Alloc(get_size());
    } else {
      own_mem_ = false;
      use_stack_buff_ = true;
      mem_.Reset(stack_buff_);
    }
    InitMsg();
  }

  explicit AppThreadDeregMsg(void *msg):
    NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kAppThreadDereg;
  }
};

struct ClientShutDownMsg : public NumberedMsg {
public:
  ClientShutDownMsg() {
    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
      own_mem_ = true;
      use_stack_buff_ = false;
      mem_.Alloc(get_size());
    } else {
      own_mem_ = false;
      use_stack_buff_ = true;
      mem_.Reset(stack_buff_);
    }
    InitMsg();
  }

  explicit ClientShutDownMsg(void *msg):
    NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kClientShutDown;
  }
};

struct ServerShutDownAckMsg : public NumberedMsg {
public:
  ServerShutDownAckMsg() {
    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
      own_mem_ = true;
      use_stack_buff_ = false;
      mem_.Alloc(get_size());
    } else {
      own_mem_ = false;
      use_stack_buff_ = true;
      mem_.Reset(stack_buff_);
    }
    InitMsg();
  }

  explicit ServerShutDownAckMsg(void *msg):
    NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kServerShutDownAck;
  }
};

//struct BgClockMsg : public NumberedMsg {
//public:
//  BgClockMsg() {
//    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
//       own_mem_ = true;
//       use_stack_buff_ = false;
//       mem_.Alloc(get_size());
//    } else {
//      own_mem_ = false;
//      use_stack_buff_ = true;
//      mem_.Reset(stack_buff_);
//    }
//    InitMsg();
//  }
//
//  explicit BgClockMsg(void *msg):
//    NumberedMsg(msg) {}
//
//protected:
//  void InitMsg() {
//    NumberedMsg::InitMsg();
//    get_msg_type() = kBgClock;
//  }
//};
//
//struct BgSendOpLogMsg : public NumberedMsg {
//public:
//  BgSendOpLogMsg() {
//    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
//       own_mem_ = true;
//       use_stack_buff_ = false;
//       mem_.Alloc(get_size());
//    } else {
//      own_mem_ = false;
//      use_stack_buff_ = true;
//      mem_.Reset(stack_buff_);
//    }
//    InitMsg();
//  }
//
//  explicit BgSendOpLogMsg(void *msg):
//    NumberedMsg(msg) {}
//
//protected:
//  void InitMsg() {
//    NumberedMsg::InitMsg();
//    get_msg_type() = kBgSendOpLog;
//  }
//};

// This is a special type of message which transfers the ownership of a
// piece of memory from sender to receiver. That means the receiver knows
// how to interpret the memory and more importantly, knows how and will
// destroy the memory.
struct MemTransferMsg : public NumberedMsg {
public:
  MemTransferMsg() {
    if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
       own_mem_ = true;
       use_stack_buff_ = false;
       mem_.Alloc(get_size());
    } else {
      own_mem_ = false;
      use_stack_buff_ = true;
      mem_.Reset(stack_buff_);
    }
    InitMsg();
  }

  explicit MemTransferMsg(void *msg):
    NumberedMsg(msg) {}

  size_t get_size() {
    return NumberedMsg::get_size() + sizeof(void*);
  }

  void*& get_mem_ptr() {
    return *(reinterpret_cast<void**>(mem_.get_mem()
      + NumberedMsg::get_size()));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kMemTransfer;
  }
};

struct ArbitrarySizedMsg : public NumberedMsg {
public:
  ArbitrarySizedMsg() {}

  explicit ArbitrarySizedMsg(int32_t avai_size) {
    own_mem_ = true;
    mem_.Alloc(get_header_size() + avai_size);
    InitMsg(avai_size);
  }

  explicit ArbitrarySizedMsg(void *msg):
    NumberedMsg(msg) {}

  virtual size_t get_header_size() {
    return NumberedMsg::get_size() + sizeof(size_t);
  }

  size_t &get_avai_size() {
    return *(reinterpret_cast<size_t*>(mem_.get_mem()
      + NumberedMsg::get_size()));
  }

  // won't be available until the object is constructed
  virtual size_t get_size() {
    return get_header_size() + get_avai_size();
  }

protected:
  virtual void InitMsg(int32_t avai_size){
    NumberedMsg::InitMsg();
    get_avai_size() = avai_size;
  }
};

//struct ServerRowRequestReplyMsg : public ArbitrarySizedMsg {
//public:
//  explicit ServerRowRequestReplyMsg(int32_t avai_size) {
//    own_mem_ = true;
//    mem_.Alloc(get_header_size() + avai_size);
//    InitMsg(avai_size);
//  }
//
//  explicit ServerRowRequestReplyMsg(void *msg):
//    ArbitrarySizedMsg(msg) {}
//
//  size_t get_header_size() {
//    return ArbitrarySizedMsg::get_header_size() + sizeof(int32_t)
//      + sizeof(int32_t) + sizeof(int32_t) + sizeof(uint32_t) + sizeof(size_t);
//  }
//
//  int32_t &get_table_id() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size()));
//  }
//
//  int32_t &get_row_id() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size() + sizeof(int32_t) ));
//  }
//
//  int32_t &get_clock() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size()
//      + sizeof(int32_t) + sizeof(int32_t) ));
//  }
//
//  uint32_t &get_version() {
//    return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size()
//      + sizeof(int32_t) + sizeof(int32_t) +sizeof(int32_t)));
//  }
//
//  size_t &get_row_size() {
//    return *(reinterpret_cast<size_t*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size()
//      + sizeof(int32_t) + sizeof(int32_t) +sizeof(int32_t) + sizeof(uint32_t)));
//  }
//
//  void *get_row_data() {
//    return mem_.get_mem() + get_header_size();
//  }
//
//  size_t get_size() {
//    return get_header_size() + get_avai_size();
//  }
//
//protected:
//  virtual void InitMsg(int32_t avai_size) {
//    ArbitrarySizedMsg::InitMsg(avai_size);
//    get_msg_type() = kServerRowRequestReply;
//  }
//};
//
//struct ClientSendOpLogMsg : public ArbitrarySizedMsg {
//public:
//  explicit ClientSendOpLogMsg(int32_t avai_size) {
//    own_mem_ = true;
//    mem_.Alloc(get_header_size() + avai_size);
//    InitMsg(avai_size);
//  }
//
//  explicit ClientSendOpLogMsg(void *msg):
//    ArbitrarySizedMsg(msg) {}
//
//  size_t get_header_size() {
//    return ArbitrarySizedMsg::get_header_size() + sizeof(bool)
//      + sizeof(int32_t) + sizeof(uint32_t);
//  }
//
//  bool &get_is_clock() {
//    return *(reinterpret_cast<bool*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size()));
//  }
//
//  int32_t &get_client_id() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size() + sizeof(bool)));
//  }
//
//  uint32_t &get_version() {
//    return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size() + sizeof(bool)
//      + sizeof(int32_t)));
//  }
//
//  // data is to be accessed via SerializedOpLogAccessor
//  void *get_data() {
//    return mem_.get_mem() + get_header_size();
//  }
//
//  size_t get_size() {
//    return get_header_size() + get_avai_size();
//  }
//
//protected:
//  virtual void InitMsg(int32_t avai_size) {
//    ArbitrarySizedMsg::InitMsg(avai_size);
//    get_msg_type() = kClientSendOpLog;
//  }
//};

/*
NOTE(jiyuan): add ClientSendOpLogIterationMsg type
1, Similar to ClientSendOpLogMsg, but here it uses |iteration| rather than
|version| to denote the ID of its progress.
2, it uses is_clock to mark whether this batch of OpLog is the final part of an iteration

v-feigao: add get_server_id
1, delta_io_thread uses this item to tell bg_workers which server to send
*/

struct ClientSendOpLogIterationMsg : public ArbitrarySizedMsg {
public:
	explicit ClientSendOpLogIterationMsg(int32_t avai_size) {
		own_mem_ = true;
		mem_.Alloc(get_header_size() + avai_size);
		InitMsg(avai_size);
	}

	explicit ClientSendOpLogIterationMsg(void *msg) :
		ArbitrarySizedMsg(msg) {}

	size_t get_header_size() {
		return ArbitrarySizedMsg::get_header_size() + sizeof(bool)+sizeof(int32_t)
			+ sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(bool);
	}

	bool &get_is_clock() {
		return *(reinterpret_cast<bool*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size()));
	}

	int32_t &get_client_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)));
	}

	/*
	uint32_t &get_version() {
		return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)
			+sizeof(int32_t)));
	}
	*/
	int32_t &get_iteration() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)
			+sizeof(int32_t)));
	}

	int32_t &get_server_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)
			+sizeof(int32_t) + sizeof(int32_t)));
	}

	int32_t &get_app_thread_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)
			+sizeof(int32_t)+sizeof(int32_t) + sizeof(int32_t)));
	}

	int32_t &get_table_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)
			+sizeof(int32_t)+sizeof(int32_t)+sizeof(int32_t)+sizeof(int32_t)));
	}

	bool &get_is_iteration_clock() {
		return *(reinterpret_cast<bool*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)
			+sizeof(int32_t)+sizeof(int32_t)+sizeof(int32_t)+sizeof(int32_t) +sizeof(int32_t)));
	}
	// data is to be accessed via SerializedOpLogAccessor
	void *get_data() {
		return mem_.get_mem() + get_header_size();
	}

	size_t get_size() {
		return get_header_size() + get_avai_size();
	}

protected:
	virtual void InitMsg(int32_t avai_size) {
		ArbitrarySizedMsg::InitMsg(avai_size);
		get_msg_type() = kClientSendOpLogIteration;
	}
};

//struct ServerPushRowMsg : public ArbitrarySizedMsg {
//public:
//  explicit ServerPushRowMsg(int32_t avai_size) {
//    own_mem_ = true;
//    mem_.Alloc(get_header_size() + avai_size);
//    InitMsg(avai_size);
//  }
//
//  explicit ServerPushRowMsg(void *msg):
//    ArbitrarySizedMsg(msg) {}
//
//  size_t get_header_size() {
//    return ArbitrarySizedMsg::get_header_size() + sizeof(int32_t)
//        + sizeof(uint32_t) + sizeof(bool);
//  }
//
//  int32_t &get_clock() {
//    return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size()));
//  }
//
//  uint32_t &get_version() {
//    return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size() + sizeof(int32_t)));
//  }
//
//  bool &get_is_clock() {
//    return *(reinterpret_cast<bool*>(mem_.get_mem()
//      + ArbitrarySizedMsg::get_header_size() + sizeof(int32_t)
//      + sizeof(uint32_t) ));
//  }
//
//  // data is to be accessed via SerializedRowReader
//  void *get_data() {
//    return mem_.get_mem() + get_header_size();
//  }
//
//  size_t get_size() {
//    return get_header_size() + get_avai_size();
//  }
//
//protected:
//  virtual void InitMsg(int32_t avai_size) {
//    ArbitrarySizedMsg::InitMsg(avai_size);
//    get_msg_type() = kServerPushRow;
//  }
//};

/*
NOTE(jiyuan): add ServerPushOpLogIterationMsg
Similar to ServerPushRowMsg and ClientSendOpLogIterationMsg in that
1, it sends the aggregated OpLog from servers to clients
2, it uses |iteration| to indicate the iteration of its aggregated updates
3, uses is_clock to mark whether it is the final part of a particular iteration

v-feigao: add method get_bg_id: 
1, aggregator thread uses this item to tell server_thread which bg_id to send
*/

struct ServerPushOpLogIterationMsg : public ArbitrarySizedMsg {
public:
	explicit ServerPushOpLogIterationMsg(int32_t avai_size) {
		own_mem_ = true;
		mem_.Alloc(get_header_size() + avai_size);
		InitMsg(avai_size);
	}

	explicit ServerPushOpLogIterationMsg(void *msg) :
		ArbitrarySizedMsg(msg) {}

	size_t get_header_size() {
		return ArbitrarySizedMsg::get_header_size() + sizeof(bool) + 
			sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t)+sizeof(int32_t);
	}

	/*
	int32_t &get_clock() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size()));
	}

	uint32_t &get_version() {
		return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(int32_t)));
	}

	bool &get_is_clock() {
		return *(reinterpret_cast<bool*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(int32_t)
			+sizeof(uint32_t)));
	}
	*/
	bool &get_is_clock() {
		return *(reinterpret_cast<bool*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size()));
	}

	int32_t &get_server_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)));
	}

	int32_t &get_iteration() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool) + sizeof(int32_t)));
	}

	int32_t &get_bg_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + 
			sizeof(bool)+sizeof(int32_t)+sizeof(int32_t)));
	}

	int32_t &get_table_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + 
			sizeof(bool)+sizeof(int32_t)+sizeof(int32_t)+sizeof(int32_t)));
	}

	// data is to be accessed via SerializedRowReader
	void *get_data() {
		return mem_.get_mem() + get_header_size();
	}

	size_t get_size() {
		return get_header_size() + get_avai_size();
	}

protected:
	virtual void InitMsg(int32_t avai_size) {
		ArbitrarySizedMsg::InitMsg(avai_size);
		get_msg_type() = kServerPushOpLogIteration;
	}
};

// v-feigao: The following will not be used.
// v-feigao: add BgSendOpLogIterationMsg
// 1. Delta IO thread create and send BgSendOpLogIterationMsg to BgWorkers.
// 2. Msg.get_server_id() tell to BgWorkers which server to send.
// 3. Msg.get_data() gives the ClientSendOpLogIterationMsg.
struct BgSendOpLogIterationMsg : public ArbitrarySizedMsg {
public:
	explicit BgSendOpLogIterationMsg(int32_t avai_size) {
		own_mem_ = true;
		mem_.Alloc(get_header_size() + avai_size);
		InitMsg(avai_size);
	}

	explicit BgSendOpLogIterationMsg(void *msg) :
		ArbitrarySizedMsg(msg) {}

	size_t get_header_size() {
		return ArbitrarySizedMsg::get_header_size() + sizeof(bool)
			+sizeof(int32_t)+sizeof(int32_t);
	}

	bool &get_is_clock() {
		return *(reinterpret_cast<bool*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size()));
	}

	int32_t &get_server_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)));
	}

	/*
	uint32_t &get_version() {
	return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
	+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)
	+sizeof(int32_t)));
	}
	*/
	int32_t &get_iteration() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size() + sizeof(bool)
			+sizeof(int32_t)));
	}

	// data is to be accessed via SerializedOpLogAccessor
	void *get_data() {
		return mem_.get_mem() + get_header_size();
	}

	size_t get_size() {
		return get_header_size() + get_avai_size();
	}

protected:
	virtual void InitMsg(int32_t avai_size) {
		ArbitrarySizedMsg::InitMsg(avai_size);
		get_msg_type() = kBgSendOpLogIteration;
	}
};

// Note(v-feigao): add kClientModelSliceRequest msg
// old version: just request slice_id, no local vocab
//struct ClientModelSliceRequestMsg : public NumberedMsg {
//public:
//	ClientModelSliceRequestMsg() {
//		if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
//			own_mem_ = true;
//			use_stack_buff_ = false;
//			mem_.Alloc(get_size());
//		}
//		else {
//			own_mem_ = false;
//			use_stack_buff_ = true;
//			mem_.Reset(stack_buff_);
//		}
//		InitMsg();
//	}
//
//	explicit ClientModelSliceRequestMsg(void *msg) :
//		NumberedMsg(msg) {}
//
//	size_t get_size() {
//		return NumberedMsg::get_size() + sizeof(int32_t)+sizeof(int32_t)
//			+sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t);
//	}
//
//	int32_t &get_table_id() {
//		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//			+ NumberedMsg::get_size()));
//	}
//
//
//	int32_t &get_slice_id() {
//		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//			+ NumberedMsg::get_size() + sizeof(int32_t)));
//	}
//
//	int32_t &get_clock() {
//		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//			+ NumberedMsg::get_size() + sizeof(int32_t)+sizeof(int32_t)));
//	}
//
//	int32_t &get_client_id() {
//		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//			+ NumberedMsg::get_size() + sizeof(int32_t)+sizeof(int32_t)+sizeof(int32_t)));
//	}
//
//	int32_t &get_server_id() {
//		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//			+ NumberedMsg::get_size() + sizeof(int32_t)+sizeof(int32_t)+sizeof(int32_t)+sizeof(int32_t)));
//	}
//protected:
//	void InitMsg() {
//		NumberedMsg::InitMsg();
//		get_msg_type() = kClientModelSliceRequest;
//	}
//};

struct ClientModelSliceRequestMsg : public ArbitrarySizedMsg {
public:
	explicit ClientModelSliceRequestMsg(int32_t avai_size) {
		own_mem_ = true;
		mem_.Alloc(get_header_size() + avai_size);
		InitMsg(avai_size);
	}

	explicit ClientModelSliceRequestMsg(void *msg) :
		ArbitrarySizedMsg(msg) {}

	size_t get_header_size() {
		return ArbitrarySizedMsg::get_header_size()
			+sizeof(int32_t);
	}

	int32_t &get_client_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ ArbitrarySizedMsg::get_header_size()));
	}

	// data is format as:
	// num_of_words, word1, word2, ... , word n.
	void *get_data() {
		return mem_.get_mem() + get_header_size();
	}

	size_t get_size() {
		return get_header_size() + get_avai_size();
	}

protected:
	virtual void InitMsg(int32_t avai_size) {
		ArbitrarySizedMsg::InitMsg(avai_size);
		get_msg_type() = kClientModelSliceRequest;
	}
};

//Note(v-feigao): add kServerModelSliceRequestReply Msg
//struct ServerModelSliceRequestReplyMsg : public ArbitrarySizedMsg {
//public:
//	explicit ServerModelSliceRequestReplyMsg(int32_t avai_size) {
//		own_mem_ = true;
//		mem_.Alloc(get_header_size() + avai_size);
//		InitMsg(avai_size);
//	}
//
//	explicit ServerModelSliceRequestReplyMsg(void *msg) :
//		ArbitrarySizedMsg(msg) {}
//
//	size_t get_header_size() {
//		return ArbitrarySizedMsg::get_header_size() + sizeof(int32_t)
//			+sizeof(int32_t)+sizeof(bool);
//	}
//
//	int32_t &get_clock() {
//		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//			+ ArbitrarySizedMsg::get_header_size()));
//	}
//
//	int32_t &get_table_id() {
//		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
//			+ ArbitrarySizedMsg::get_header_size() + sizeof(int32_t)));
//	}
//
//	bool &get_is_clock() {
//		return *(reinterpret_cast<bool*>(mem_.get_mem()
//			+ ArbitrarySizedMsg::get_header_size() + sizeof(int32_t)
//			+sizeof(uint32_t)));
//	}
//
//	// data is to be accessed via SerializedRowReader
//	void *get_data() {
//		return mem_.get_mem() + get_header_size();
//	}
//
//	size_t get_size() {
//		return get_header_size() + get_avai_size();
//	}
//
//protected:
//	virtual void InitMsg(int32_t avai_size) {
//		ArbitrarySizedMsg::InitMsg(avai_size);
//		get_msg_type() = kServerModelSliceRequestReply;
//	}
//};

// Note(v-feigao): add server finish init msg

struct ServerUpdateClockMsg : public NumberedMsg {
public:
	ServerUpdateClockMsg() {
		if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
			own_mem_ = true;
			use_stack_buff_ = false;
			mem_.Alloc(get_size());
		}
		else {
			own_mem_ = false;
			use_stack_buff_ = true;
			mem_.Reset(stack_buff_);
		}
		InitMsg();
	}

	explicit ServerUpdateClockMsg(void *msg) :
		NumberedMsg(msg) {}

	size_t get_size() {
		return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t);
	}

	int32_t &get_iteration() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ NumberedMsg::get_size()));
	}

	int32_t &get_server_id() {
		return *(reinterpret_cast<int32_t*>(mem_.get_mem()
			+ NumberedMsg::get_size() + sizeof(int32_t)));
	}

protected:
	void InitMsg(){
		NumberedMsg::InitMsg();
		get_msg_type() = kServerUpdateClock;
	}
};
}  // namespace petuum
