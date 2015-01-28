// Author: Gao Fei(v-feigao@microsoft.com)
// Data: 2014-10-13

#pragma once

#include <map>
#include <utility>
#include <vector>
#include <unordered_map>

#include "system/system_context.hpp"

namespace petuum {

	class SummaryDelta {
		friend class ClientSummaryRow;
	public:
		SummaryDelta() {
			K_ = petuum::GlobalContext::get_num_topics();
			delta_ = new int32_t[K_]();
		}

		~SummaryDelta() {
			delete[] delta_;
		}

		void Update(int32_t topic, int32_t delta) {
			CHECK(topic < K_);
			delta_[topic] += delta;
		}

		void Clear() {
			memset(delta_, 0, sizeof(int32_t)* K_);
		}

	private:
		int32_t K_;
		int32_t* delta_;
	};

	class DeltaArray {
		// friend class DeltaTable;
		// friend class DeltaDenseTable;
		friend class ClientWordTopicTable;
		friend class ClientSummaryRow;
	public:
		struct Delta {
			int32_t word_id;
			int32_t topic_id;
			int32_t delta;
		};

		const int32_t kReserveSize = 0x300000; // sizeof(Delta) = 12B. 12B * 3M = 36MB

		DeltaArray() :index_(0), is_clock_(false) {
			array_ = new Delta[kReserveSize];
		}

		DeltaArray(int32_t table_id, int32_t iteration = -1) :
			index_(0), is_clock_(false), table_id_(table_id), iteration_(iteration)
		{
			array_ = new Delta[kReserveSize];
		}

		~DeltaArray() {
			delete[] array_;
		}

		inline void Clear() {
			index_ = 0;
		}

		inline int32_t TableId() const { return table_id_; }

		inline void SetTableId(int32_t table_id) { table_id_ = table_id; }

		inline bool Clock() const { return is_clock_; }

		inline void SetClock(bool is_clock) { is_clock_ = is_clock; }

		inline int32_t Iteration() const { return iteration_; }

		inline void SetIteration(int32_t iteration) { iteration_ = iteration; }

		inline int32_t ThreadId() const { return thread_id_; }

		inline void SetThreadId(int32_t thread_id) { thread_id_ = thread_id; }

		inline int32_t SliceID() { return slice_id_; }

		inline void SetSliceID(int32_t slice_id) { slice_id_ = slice_id; }

		inline int32_t BatchID() { return batch_id_; }

		inline void SetBatchID(int32_t batch_id) { batch_id_ = batch_id; }

		void SetProperty(int32_t thread_id, 
			int32_t iteration, 
			int32_t batch_id,
			int32_t slice_id, 
			bool clock) {
			thread_id_ = thread_id;
			iteration_ = iteration;
			batch_id_ = batch_id;
			slice_id_ = slice_id;
			is_clock_ = clock;
		}

		inline bool ValidDocSize(int32_t doc_size) { return doc_size * 2 < kReserveSize - index_; }

		inline void Update(int32_t word_id, int32_t topic_id, int32_t delta) {
			CHECK_LE(index_, kReserveSize);
			array_[index_].word_id = word_id;
			array_[index_].topic_id = topic_id;
			array_[index_].delta = delta;
			++index_;
		}


	public:
		Delta* array_;
		int32_t index_;

		bool is_clock_;
		int32_t table_id_;
		int32_t iteration_;
		int32_t thread_id_;
		int32_t batch_id_;
		int32_t slice_id_;
	};
}