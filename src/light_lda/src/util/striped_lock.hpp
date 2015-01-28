// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2014.01.23

#pragma once


#include "util/lock.hpp"
#include "util/lockable.hpp"
#include <mutex>
#include <cstdint>
#include <boost/scoped_array.hpp>
#include <boost/utility.hpp>
#include <glog/logging.h>
#include "utils.hpp"

namespace petuum {

namespace {

// We will use lock pool of size (num_threads * kLockPoolSizeExpansionFactor).
int32_t kLockPoolSizeExpansionFactor = 20;

}   // anonymous namespace

// StripedLock does not support scoped lock. MUTEX must implement Lockable
// interface.
template<typename K,
typename MUTEX = std::mutex,
typename HASH = std::hash<K> >
class StripedLock : boost::noncopyable {
public:
  // Determine number of locks based on number of cores.
	StripedLock() :
		//StripedLock(get_nprocs_conf() * kLockPoolSizeExpansionFactor) { }
		StripedLock(get_CPU_core_num() * kLockPoolSizeExpansionFactor) {LOG(FATAL)<<"---CPU"<<get_CPU_core_num() * kLockPoolSizeExpansionFactor;}

  // Initialize with number of locks in the pool.
  explicit StripedLock(int lock_pool_size) :
    lock_pool_size_(lock_pool_size),
    lock_pool_(new MUTEX[lock_pool_size_]) {
    //VLOG(0) << "Lock pool size: " << lock_pool_size_;
    LOG(ERROR) << "Lock pool size: " << lock_pool_size_;
    }

  // Lock index idx.
  inline void Lock(K idx) {
    int lock_idx = hasher(idx) % lock_pool_size_;
    lock_pool_[lock_idx].lock();
  }

  // Lock index idx, and let unlocker unlock it later on.
  inline void Lock(K idx, Unlocker<MUTEX>* unlocker) {
      if (lock_pool_size_ < 1){
    fprintf(stderr, "-----------Lock pool size: %d\n",lock_pool_size_);
    fprintf(stderr, "-----------Test1Lock pool size: %d\n",test1);
    fprintf(stderr, "-----------Test2Lock pool size: %d\n",test2);
      fflush(stderr);
      }
   
    int lock_idx = hasher(idx) % lock_pool_size_;
    lock_pool_[lock_idx].lock();
    unlocker->SetLock(&lock_pool_[lock_idx]);
  }

  // Lock index idx.
  inline bool TryLock(K idx) {
    int lock_idx = hasher(idx) % lock_pool_size_;
    return lock_pool_[lock_idx].try_lock();
  }

  // Lock index idx.
  inline bool TryLock(K idx, Unlocker<MUTEX>* unlocker) {
    int lock_idx = hasher(idx) % lock_pool_size_;
    if (lock_pool_[lock_idx].try_lock()) {
      unlocker->SetLock(&lock_pool_[lock_idx]);
      return true;
    }
    return false;
  }

  // Unlock.
  inline void Unlock(K idx) {
    int lock_idx = hasher(idx) % lock_pool_size_;
    lock_pool_[lock_idx].unlock();
  }

private:
  // Use default HASH function.
  static HASH hasher;

  // Size of the pool. Currently does not support resizing.
  int test1 = 20;
  const int32_t lock_pool_size_;
  int test2 = 30;

  // pool of mutexes.
  boost::scoped_array<MUTEX> lock_pool_;
};

//template<typename K,
//	typename MUTEX = std::mutex,
//	typename HASH = std::hash<K> >
//	HASH StripedLock<K, MUTEX, HASH>::hasher;
template<typename K,
	typename MUTEX,
	typename HASH >
	HASH StripedLock<K, MUTEX, HASH>::hasher;


}  // namespace petuum
