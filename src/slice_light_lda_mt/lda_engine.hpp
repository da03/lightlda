// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2014.03.29

#pragma once

#include <memory>
#include "light_doc_sampler.hpp"
#include "context.hpp"
#include <cstdint>
#include <utility>
#include <vector>
#include <string>
#include <set>
#include <atomic>
#include <boost/thread/barrier.hpp>
#include <mutex>
#include "data_block.h"
#include "hybrid_map.h"
#include "alias_multinomial_rng_int.hpp"


namespace lda {

	// Engine takes care of the entire pipeline of LDA, from reading data to
	// spawning threads, to recording execution time and loglikelihood.
	class LDAEngine {
	public:
		LDAEngine();

		// Read libSVM formatted document (.dat) data. This should only be called
		// once by one thread.  Returning # of vocabs.
		int32_t ReadData(const std::string& doc_file);

		void Start();
		void Dump(LightDocSampler &sampler_, int iter, int thread_id);

	private:  // private data

		// Number of topics
		int32_t K_;   

		// Number of vocabs.
		int32_t V_;

		// vocabs in this data partition.
		std::set<int32_t> local_vocabs_;

		// Compute complete log-likelihood (ll) every compute_ll_interval_
		// iterations.
		std::atomic<double> doc_ll_;
		std::atomic<double> word_ll_;
		int32_t compute_ll_interval_;

		std::vector<DocBatch> docs_;
		std::unique_ptr<lda::LDADataBlock> data_block_;

		// # of tokens processed in a Clock() call.
		std::atomic<int32_t> num_tokens_clock_;
		std::atomic<int32_t> thread_counter_;

		// Local barrier across threads.
		std::unique_ptr<boost::barrier> process_barrier_;

		std::mutex global_mutex_;
		lda::LDAModelBlock model_block_;
		std::vector<lda::hybrid_map> global_word_topic_table_;

		real_t beta_;
		real_t beta_sum_;

		wood::AliasMultinomialRNGInt alias_rng_int_;
		int32_t beta_height_;
		float beta_mass_;
		std::vector<wood::alias_k_v> beta_k_v_;

		std::vector<lda::hybrid_alias_map> global_alias_k_v_;
		
		std::vector<int64_t> global_summary_row_;

		int32_t slice_num_;
		std::vector<int32_t> slice_range_;
		std::vector<int32_t> word_range_for_each_thread_;

		LightDocSampler **samplers_;
	};

}   // namespace lda
