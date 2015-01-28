#pragma once

#include <vector>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <memory>

// <ADDED>
namespace std {
template<typename T, typename... Args>
    std::unique_ptr<T> make_unique(Args&&... args) {
            return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
    }
};
// </ADDED>
namespace lda {

	// A concurrency DoubleBuffer shared by multiple app threads and one IO thread.
	// Both app threads and IO thread should call Start(thread_id) method before
	// getting access to the buffer and End(thread_id) after finishing operation on buffer. 
	// Any operation outside the range between Start and End are undefined.
	template<class Container>
	class DoubleBuffer {
	public:
		// num_thread: number of WORKER thread that access double buffer.
		DoubleBuffer(int num_threads) : num_threads_(num_threads + 1), counter_(num_threads) {
			reader_buffer_ = std::make_unique<Container>();
			writer_buffer_ = std::make_unique<Container>();
			ready_.resize(num_threads_); // plus ONE IO thread.
			ready_[0] = true; // id 0 is io thread by default.
			exit_ = false;
			//ready_ = false;
		}

		~DoubleBuffer() {
		}

		void Exit() {
			std::unique_lock<std::mutex> lock(mutex_);
			exit_ = true;
			condition_.notify_one();
		}

		void Start(int thread_id);
		void End(int thread_id);

		std::unique_ptr<const Container> ImmutableWorkerBuffer() const {
			return reader_buffer_;
		}
		std::unique_ptr<Container>& MutableWorkerBuffer()  {
			return reader_buffer_;
		}
		
		std::unique_ptr<Container>& MutableIOBuffer()  {
			return writer_buffer_;
		}
		
	private:
		int num_threads_; // num of total threads that have access to this buffer. 
		std::unique_ptr<Container> reader_buffer_;
		std::unique_ptr<Container> writer_buffer_;
		
		std::mutex mutex_;
		std::condition_variable condition_;
		std::vector<bool> ready_;
		std::atomic_bool exit_;
		//bool ready_;
		std::atomic<int> counter_;

		DoubleBuffer(const DoubleBuffer&);
		void operator=(const DoubleBuffer&);
	};

	template<class Container>
	void DoubleBuffer<Container>::Start(int thread_id) {
		std::unique_lock<std::mutex> lock(mutex_);
		condition_.wait(lock, [&](){ return ready_[thread_id] == true || exit_ == true; });
		//condition_.wait(lock, [this]{ return ready_});
	}

	template<class Container>
	void DoubleBuffer<Container>::End(int thread_id) {
		std::unique_lock<std::mutex> lock(mutex_);
		ready_[thread_id] = false;
		if (++counter_ == num_threads_) {
			counter_ = 0;
			std::swap(reader_buffer_, writer_buffer_);
			std::fill(ready_.begin(), ready_.end(), true);
			condition_.notify_all();
		}
	}

	// RAII Class for Buffer Access
	// Buf should have method Start and End
	template<class Buf>
	class BufferGuard {
	public:
		BufferGuard(Buf& buffer, int thread_id) : buffer_(buffer), thread_id_(thread_id) {
			buffer_.Start(thread_id_);
		}
		~BufferGuard() {
			buffer_.End(thread_id_);
		}
	private:
		Buf& buffer_;
		int thread_id_;
	};
};
