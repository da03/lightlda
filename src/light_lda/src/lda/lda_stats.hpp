// Author: Gao Fei (v-feigao@microsoft.com)
// Date: 2014.10.20

#pragma once

#include <string>
#include "base/common.hpp"
#include "memory/data_block.h"
#include "memory/model_slice.h"
#include "memory/summary_row.hpp"
#include "util/delta_table.h"

namespace lda {
	// We reference Griffiths and Steyvers (PNAS, 2003).
	class LDAStats {
	public:
		LDAStats();

		void Init(LocalVocab* local_vocab, int32_t slice_id) {
			local_vocab_ = local_vocab;
			slice_id_ = slice_id;
		}
		// The i-th complete-llh calculation will use row i in llh_able_. This is
		// part of log P(z) in eq.[3].
		double ComputeOneDocLLH(LDADocument* doc);

		double ComputeOneSliceWordLLH(
			ModelSlice& word_topic_table,
			int32_t thread_id);

		double NormalizeWordLLH(petuum::ClientSummaryRow& summary_row);
		//// This computes the entire log P(w|z) in eq.[2].
		//// compute_ll_interval * ith_llh = iter 
		//double ComputeWordLLH(WordTopicTable& word_topic_table,
		//	SummaryTable& summary_table);

	private:
		int32_t Begin(int32_t thread_id) const {
			CHECK(thread_id < num_threads_);
			int32_t slice_size = local_vocab_->SliceSize(slice_id_);
			int32_t size_one_thread = slice_size / num_threads_;
			return thread_id * size_one_thread;
		}
		int32_t End(int32_t thread_id) const {
			CHECK(thread_id < num_threads_);
			int32_t slice_size = local_vocab_->SliceSize(slice_id_);
			if (thread_id == num_threads_ - 1)
				return slice_size;
			int32_t size_one_thread = slice_size / num_threads_;
			return (thread_id + 1) * size_one_thread;
		}

	private:
		// ================ Topic Model Parameters =================
		// Number of topics.
		int32_t K_;

		// Number of vocabs.
		int32_t V_;
		
		// Dirichlet prior for word-topic vectors.
		float beta_;

		// V_*beta_
		float beta_sum_;

		// Dirichlet prior for doc-topic vectors.
		float alpha_;

		// K_*alpha__
		float alpha_sum_;

		int32_t num_threads_;

		LocalVocab* local_vocab_;
		int32_t slice_id_;

		// ================ Precompute LLH Parameters =================
		// Log of normalization constant (per docoument) from eq.[3].
		double log_doc_normalizer_;

		// Log of normalization constant (per topic) from eq.[2].
		double log_topic_normalizer_;

	};

}  // namespace lda
