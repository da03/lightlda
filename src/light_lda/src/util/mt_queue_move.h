#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace util {
	// A multi-thread Queue
	// 1, unbounded
	// 2, both push and pop operations use move semantics, suppose T supports move semantics
	template<typename T>
	class MtQueueMove {
	public:
		MtQueueMove() { exit_ = false; }
		~MtQueueMove() {}

		// IMPORTANT: after push |item| in the queue, the variable |item| is still valid but uninitialized
		void Push(T& item) {
            //LOG(INFO)<<"------PUSH";
		//void Push(T item) {
			std::unique_lock<std::mutex> lock(mutex_);
			buffer_.push(std::move(item));
			empty_condition_.notify_one();
		}

		bool Pop(T& result) {
            //LOG(INFO)<<"******POP";
            //LOG(INFO)<<"mt_queue_move: Pop "<<buffer_.size();
			std::unique_lock<std::mutex> lock(mutex_);
            //LOG(INFO)<<"2mt_queue_move: Pop "<<buffer_.size();
			empty_condition_.wait(lock, [this]{ return !buffer_.empty() || exit_; });
            //LOG(INFO)<<"3mt_queue_move: Pop "<<buffer_.size();
			if (buffer_.empty())
			{
				return false;
			}
            //LOG(INFO)<<"4mt_queue_move: Pop "<<buffer_.size();
			result = std::move(buffer_.front());
            //LOG(INFO)<<"5mt_queue_move: Pop "<<buffer_.size();
			buffer_.pop();
            //LOG(INFO)<<"6mt_queue_move: Pop "<<buffer_.size();
			return true;
		}

		bool TryPop(T& result) {
			std::unique_lock<std::mutex> lock(mutex_);
			if (buffer_.empty()) {
				return false;
			}
			result = std::move(buffer_.front());
			buffer_.pop();
			return true;
		}

		size_t Size() const {
			std::unique_lock<std::mutex> lock(mutex_);
			return buffer_.size();
		}

		bool Empty() const {
			std::unique_lock<std::mutex> lock(mutex_);
			return buffer_.empty();
		}

		void Exit() {
			std::unique_lock<std::mutex> lock(mutex_);
			exit_ = true;
			empty_condition_.notify_all();
		}

	private:
		std::queue<T> buffer_;
		mutable std::mutex mutex_;
		std::condition_variable empty_condition_;
		std::atomic_bool exit_;

		MtQueueMove(const MtQueueMove&);
		void operator=(const MtQueueMove&);
	};
};
