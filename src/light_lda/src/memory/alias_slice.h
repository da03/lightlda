// Author: Gao Fei(v-feigao@microsoft.com)
// Data: 2014-10-18

#pragma once

#include <fstream>
#include <thread>
#include <boost/thread/tss.hpp>
#include "base/common.hpp"
#include "memory/local_vocab.h"

namespace petuum {
	class ClientSummaryRow;
}

namespace wood {
	class xorshift_rng;
}

namespace lda {

	class hybrid_map;
	class ModelSlice;

	class AliasSlice {
	public:
		AliasSlice();
		~AliasSlice();

		// Must Init before called other method
		void Init(LocalVocab* local_vocab, int32_t slice_id);

		void GenerateAliasTable(
			ModelSlice& word_topic_table,
			petuum::ClientSummaryRow& summary_row,
			int32_t thread_id, 
			wood::xorshift_rng& rng);

		int32_t ProposeTopic(int32_t word, wood::xorshift_rng& rng);

	private:
		int32_t Begin(int32_t thread_id) const;

		int32_t End(int32_t thread_id) const;

		void GenerateDenseAliasRow(
			lda::hybrid_map& word_topic_row,
			petuum::ClientSummaryRow& summary_row, 
			int32_t* memory, 
			int32_t& height, 
			real_t& n_kw_mass,
			int32_t capacity,
			wood::xorshift_rng& rng);

		void GenerateSparseAliasRow(
			int32_t index, 
			lda::hybrid_map& word_topic_row,
			petuum::ClientSummaryRow& summary_row,
			int32_t* memory, 
			int32_t& height,
			real_t& n_kw_mass,
			int32_t capacity, 
			wood::xorshift_rng& rng);

		void GenerateBetaAliasRow(
			petuum::ClientSummaryRow& summary_row,
			wood::xorshift_rng& rng);

	private:
		int32_t* memory_block_;
		int64_t memory_block_size_;
		std::vector<int32_t> height_;
		std::vector<real_t> n_kw_mass_;
		int32_t beta_height_;
		real_t beta_mass_;

		// beta dense alias
		std::vector<int32_t> beta_k_;
		std::vector<int32_t> beta_v_;

		LocalVocab* local_vocab_;
		int32_t slice_id_;

		// thread specific aux variable
		boost::thread_specific_ptr<std::vector<real_t>> q_w_proportion_;
		boost::thread_specific_ptr<std::vector<int32_t>> q_w_proportion_int_;
		boost::thread_specific_ptr<std::vector<std::pair<int32_t, int32_t>>> L_;
		boost::thread_specific_ptr<std::vector<std::pair<int32_t, int32_t>>> H_;

		int32_t K_;
		int32_t V_;
		real_t beta_;
		real_t beta_sum_;
		int32_t num_threads_;
	};

	inline int32_t AliasSlice::Begin(int32_t thread_id) const {
		int32_t slice_size = local_vocab_->SliceSize(slice_id_);
		int32_t size_one_thread = slice_size / num_threads_;
		return thread_id * size_one_thread;
	}

	inline int32_t AliasSlice::End(int32_t thread_id) const {
		int32_t slice_size = local_vocab_->SliceSize(slice_id_);
		if (thread_id == num_threads_ - 1)
			return slice_size;
		int32_t size_one_thread = slice_size / num_threads_;
		return (thread_id + 1) * size_one_thread;
	}

}