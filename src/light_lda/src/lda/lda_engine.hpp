// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2014.03.29

#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <set>
#include <thread>
#include <utility>
#include "base/common.hpp"
#include "lda/context.hpp"
#include "memory/data_block.h"
#include "memory/local_vocab.h"
#include "memory/model_slice.h"
#include "memory/alias_slice.h"
#include "memory/delta_slice.h"
#include "memory/summary_row.hpp"
#include "system/ps_msgs.hpp"
#include "util/delta_table.h"
#include "util/vector_clock.hpp"
#include "util/delta_pool.h"
#include "util/double_buffer.h"


namespace lda {

	// Engine takes care of the entire pipeline of LDA, from reading data to
	// spawning threads, to recording execution time and loglikelihood.
	class LDAEngine {
	public:
		LDAEngine();

		void Setup();

		void Train();
		void Test();
	
	private:
		void DataIOThreadFunc();
		void ModelIOThreadFunc();
		void DeltaIOThreadFunc();

		void RequestModelSlice(int32_t slice_id, 
			const LocalVocab& local_vocab);

		static void DeltaSendMsg(
			int32_t recv_id,
			petuum::ClientSendOpLogIterationMsg* msg,
			bool is_last,
			bool is_iteration_clock);


	private:  
		int32_t K_;   
		int32_t V_;
		int32_t num_threads_;
		int32_t num_delta_threads_; // v-feigao: multi-delta threads

		int32_t num_iterations_;
		int32_t compute_ll_interval_;
		bool cold_start_;

		std::mutex llh_mutex_;
		std::thread data_io_thread_;
		std::thread model_io_thread_;
		std::vector<std::thread> delta_io_threads_; // v-feigao: multi-delta threads
		
		std::atomic<int32_t> thread_counter_;
		std::atomic<int32_t> delta_thread_counter_;
		std::atomic_bool delta_inited_;
		std::mutex delta_mutex_;
		
		std::unique_ptr<boost::barrier> process_barrier_;
		std::unique_ptr<boost::barrier> process_barrier_all_;
		std::unique_ptr<boost::barrier> process_barrier_delta_; // v-feigao: multi-delta threads

		std::atomic<int64_t> num_tokens_clock_;
		std::atomic<int64_t> num_delta_clock_;

		std::atomic_bool app_thread_running_;

		std::string db_file_;
		std::string vocab_file_;
		lda::Vocabs vocabs_;
		
		AliasSlice alias_slice_;

		typedef DoubleBuffer<ModelSlice> WordTopicBuffer;
		typedef DoubleBuffer<petuum::ClientSummaryRow> SummaryBuffer;
		std::unique_ptr<WordTopicBuffer> word_topic_table_;
		std::unique_ptr<SummaryBuffer> summary_row_;

		// key, block_id; value, data_block
		// typedef std::pair<int32_t, std::unique_ptr<LDADataBlock>> DataBlock; 
		typedef DoubleBuffer<LDADataBlock> DataBlockBuffer;
		std::unique_ptr<DataBlockBuffer> data_;

		std::unique_ptr<DeltaSlice> word_topic_delta_;
		std::unique_ptr<petuum::ClientSummaryRow> summary_row_delta_;

		int32_t num_blocks_;
		int32_t num_all_slice_;

		double doc_likelihood_;
		double word_likelihood_;

		typedef util::MtQueueMove<std::unique_ptr<petuum::DeltaArray>> WordTopicDeltaQueue;
		std::vector<std::unique_ptr<WordTopicDeltaQueue>> word_topic_delta_queues_; // v-feigao: multi-delta threads

		util::MtQueueMove<std::unique_ptr<petuum::SummaryDelta> > summary_delta_queue_;

		petuum::DeltaPool<petuum::DeltaArray> delta_pool_;
		petuum::DeltaPool<petuum::SummaryDelta> summary_pool_;
	};

}   // namespace lda
