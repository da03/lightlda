// Author: Gao Fei(v-feigao@microsoft.com)
// Data: 2014-10-18

#include "memory/alias_slice.h"
#include "memory/local_vocab.h"
#include "memory/model_slice.h"
#include "memory/summary_row.hpp"
#include "util/rand_int_rng.h"

namespace lda {
	AliasSlice::AliasSlice() {
		util::Context& context = util::Context::get_instance();
		
		memory_block_size_ = context.get_int64("alias_max_capacity");
		try {
			memory_block_ = new int32_t[memory_block_size_];
		}
		catch (std::bad_alloc& ba) {
			LOG(FATAL) << "Bad Alloc caught: " << ba.what();
		}

		K_ = context.get_int32("num_topics");
		V_ = context.get_int32("num_vocabs");
		num_threads_ = context.get_int32("num_worker_threads");
		beta_ = context.get_double("beta");
		beta_sum_ = beta_ * V_;

		beta_k_.resize(K_);
		beta_v_.resize(K_);

		height_.resize(V_);
		n_kw_mass_.resize(V_);
	}

	AliasSlice::~AliasSlice() {
		delete[] memory_block_;
	}
	
	void AliasSlice::Init(LocalVocab* local_vocab, int32_t slice_id) {
		CHECK(local_vocab != nullptr);
		local_vocab_ = local_vocab;
		slice_id_ = slice_id;
	}

	void AliasSlice::GenerateAliasTable(
		ModelSlice& word_topic_table,
		petuum::ClientSummaryRow& summary_row,
		int32_t thread_id, 
		wood::xorshift_rng& rng) {

		const SliceMeta& meta = local_vocab_->Meta(slice_id_);

		if (!q_w_proportion_.get()) q_w_proportion_.reset(new std::vector<real_t>(K_));
		if (!q_w_proportion_int_.get()) q_w_proportion_int_.reset(new std::vector<int32_t>(K_));
		if (!L_.get()) L_.reset(new std::vector<std::pair<int32_t, int32_t>>(K_));
		if (!H_.get()) H_.reset(new std::vector<std::pair<int32_t, int32_t>>(K_));
		
		// For each word in current index;
		int32_t range = End(thread_id);
		for (int32_t index = Begin(thread_id); index != range; ++index) {
			lda::hybrid_map& word_topic_row = word_topic_table.GetRowByIndex(index);
			int32_t& height = height_[index];
			real_t& n_kw_mass = n_kw_mass_[index];
			int32_t capacity = meta[index].alias_capacity_;
			int32_t* memory = memory_block_ + meta[index].alias_offset_;
			// word_topic_row.sorted_rehashing();
			
			if (meta[index].is_alias_dense_) {
				GenerateDenseAliasRow(word_topic_row, summary_row, memory, height, n_kw_mass, capacity, rng);

			}
			else {
				GenerateSparseAliasRow(index, word_topic_row, summary_row, memory, height, n_kw_mass, capacity, rng);
			}
		}
		if (thread_id == 0) {
			GenerateBetaAliasRow(summary_row, rng);
		}	
	}

	int32_t AliasSlice::ProposeTopic(int32_t word, wood::xorshift_rng& rng) {
		int32_t index = local_vocab_->WordToIndex(slice_id_, word);
		const SliceMeta& meta = local_vocab_->Meta(slice_id_);
		
		if (meta[index].is_alias_dense_) {
			int32_t* memory = memory_block_ + meta[index].alias_offset_;
			int32_t capacity = meta[index].alias_capacity_;

			auto sample = rng.rand();
			int idx = sample / height_[index];
			if (capacity <= idx) idx = capacity - 1;

			int32_t* p = memory + 2 * idx;
			int32_t k = *p;
			p++;
			int32_t v = *p;

			int32_t m = -(sample < v);
			return (idx & m) | (k & ~m);
		}
		else {
			int32_t* memory = memory_block_ + meta[index].alias_offset_;
			int32_t* idx_vector = memory + 2 * meta[index].alias_capacity_;
			int32_t capacity = meta[index].alias_capacity_;

			float sample = rng.rand_double() * (n_kw_mass_[index] + beta_mass_);
			if (sample < n_kw_mass_[index]) {
				auto n_kw_sample = rng.rand();

				int32_t idx = n_kw_sample / height_[index];
				if (capacity <= idx) idx = capacity - 1;

				int32_t* p = memory + 2 * idx;
				int32_t k = *p; ++p;
				int32_t v = *p;
				int32_t id = idx_vector[idx];

				int32_t m = -(n_kw_sample < v);
				return (id & m) | (idx_vector[k] & ~m);
			}
			else {
				auto beta_sample = rng.rand();

				int idx = beta_sample / beta_height_;
				int beta_size = beta_k_.size();

				if (beta_size <= idx) idx = beta_size - 1;

				int32_t k = beta_k_[idx];
				int32_t v = beta_v_[idx];

				int32_t m = -(beta_sample < v);
				return (idx & m) | (k & ~m);
			}
		}
	}

	void AliasSlice::GenerateDenseAliasRow(
		lda::hybrid_map& word_topic_row,
		petuum::ClientSummaryRow& summary_row,
		int32_t* memory,
		int32_t& height,
		real_t& n_kw_mass,
		int32_t capacity, 
		wood::xorshift_rng& rng)
	{
		
		std::vector<real_t>& q_w_proportion = *q_w_proportion_;
		std::vector<int32_t>& q_w_proportion_int = *q_w_proportion_int_;
		std::vector<std::pair<int32_t, int32_t>>& L = *L_;
		std::vector<std::pair<int32_t, int32_t>>& H = *H_;

		real_t q_w_sum = 0.0;
		
		for (int k = 0; k < K_; ++k) {
			int32_t n_tw = word_topic_row[k];
			int64_t n_t = summary_row.GetSummaryCount(k);

			real_t prop = (n_tw + beta_) / (n_t + beta_sum_);
			q_w_proportion[k] = prop;
			q_w_sum += prop;
		}

		n_kw_mass = q_w_sum;
		int32_t mass_int = 0x7FFFFFFF;

		height = mass_int / K_;
		mass_int = height * K_;
		
		int64_t mass_sum = 0;
		for (int k = 0; k < K_; ++k) {
			q_w_proportion[k] /= q_w_sum;
			q_w_proportion_int[k] = q_w_proportion[k] * mass_int;
			mass_sum += q_w_proportion_int[k];
		}
		
		if (mass_sum > mass_int) {
			int32_t more = mass_sum - mass_int;
			int id = 0;
			for (int i = 0; i < more;) {
				// int id = rng.rand_k(size);
				if (q_w_proportion_int[id] >= 1) {
					--q_w_proportion_int[id];
					++i;
				}
				id = (id + 1) % K_;
			}
		}

		if (mass_sum < mass_int) {
			int32_t more = mass_int - mass_sum;
			int id = 0;
			for (int i = 0; i < more; ++i) {
				// int id = rng.rand_k(size);
				++q_w_proportion_int[id];
				id = (id + 1) % K_;
			}
		}

		for (int k = 0; k < K_; ++k) {
			int32_t* p = memory + 2 * k;
			*p = k; ++p;
			*p = (k + 1) * height;
		}

		int32_t L_head = 0, L_tail = 0;
		int32_t H_head = 0, H_tail = 0;

		for (int k = 0; k < K_; ++k) {
			auto val = q_w_proportion_int[k];
			if (val < height) {
				L[L_tail].first = k;
				L[L_tail].second = val;
				++L_tail;
			}
			else {
				H[H_tail].first = k;
				H[H_tail].second = val;
				++H_tail;
			}
		}

		CHECK(L_tail + H_tail == K_);

		while (L_head != L_tail && H_head != H_tail) {
			auto& l_pl = L[L_head++];
			auto& h_ph = H[H_head++];

			int32_t *p = memory + 2 * l_pl.first;
			*p = h_ph.first; ++p;
			*p = l_pl.first * height + l_pl.second;

			auto sum = h_ph.second + l_pl.second;
			if (sum > 2 * height) {
				H[H_tail].first = h_ph.first;
				H[H_tail].second = sum - height;
				++H_tail;
			}
			else {
				L[L_tail].first = h_ph.first;
				L[L_tail].second = sum - height;
				++L_tail;
			}
		}
		while (L_head != L_tail) {
			auto first = L[L_head].first;
			auto second = L[L_head].second;
			int32_t* p = memory + 2 * first;
			*p = first; ++p;
			*p = first * height + second;
			++L_head;
		}
		while (H_head != H_tail) {
			auto first = H[H_head].first;
			auto second = H[H_head].second;
			int32_t* p = memory + 2 * first;
			*p = first; ++p;
			*p = first * height + second;
			++H_head;
		}
	}

	void AliasSlice::GenerateSparseAliasRow(
		int32_t index,
		lda::hybrid_map& word_topic_row,
		petuum::ClientSummaryRow& summary_row,
		int32_t* memory,
		int32_t& height,
		real_t& n_kw_mass,
		int32_t capacity, 
		wood::xorshift_rng& rng) {

		int32_t* index_vector = memory + 2 * capacity;

		CHECK(q_w_proportion_.get());
		std::vector<real_t>& q_w_proportion = *q_w_proportion_;
		std::vector<int32_t>& q_w_proportion_int = *q_w_proportion_int_;
		std::vector<std::pair<int32_t, int32_t>>& L = *L_;
		std::vector<std::pair<int32_t, int32_t>>& H = *H_;
	
		int32_t size = 0;
		real_t q_w_sum = 0.0;
		
		if (word_topic_row.is_dense_) {
			for (int k = 0; k < word_topic_row.capacity_; ++k) {
				if (word_topic_row.memory_[k] == 0) continue;
				int32_t n_tw = word_topic_row.memory_[k];
				int64_t n_t = summary_row.GetSummaryCount(k);
				q_w_proportion[size] = n_tw / (n_t + beta_sum_);
				index_vector[size] = k;
				q_w_sum += q_w_proportion[size];
				++size;
			}
		}
		else {
			for (int i = 0; i < word_topic_row.capacity_; ++i) {
				if (word_topic_row.key_[i] <= 0) 
					continue;
				int32_t topic = word_topic_row.key_[i] - 1;
				int32_t n_tw = word_topic_row.value_[i];
				int64_t n_t = summary_row.GetSummaryCount(topic);
				q_w_proportion[size] = n_tw / (n_t + beta_sum_);
				index_vector[size] = topic;
				q_w_sum += q_w_proportion[size];
				++size;
			}
		}
		n_kw_mass = q_w_sum;
		int32_t mass_int = 0x7FFFFFFF;
		
		CHECK(size == capacity);

		height = mass_int / size;
		mass_int = height * size;

		int64_t mass_sum = 0;
		for (int i = 0; i < size; ++i) {
			q_w_proportion[i] /= q_w_sum;
			q_w_proportion_int[i] = q_w_proportion[i] * mass_int;
			mass_sum += q_w_proportion_int[i];
		}

		if (mass_sum > mass_int) {
			int32_t more = mass_sum - mass_int;
			int id = 0;
			for (int i = 0; i < more;) {
				// int id = rng.rand_k(size);
				if (q_w_proportion_int[id] >= 1) {
					--q_w_proportion_int[id];
					++i;
				}
				id = (id + 1) % size;
			}
		}

		if (mass_sum < mass_int)
		{
			int32_t more = mass_int - mass_sum;
			int id = 0;
			for (int i = 0; i < more; ++i) {
				// int id = rng.rand_k(size);
				++q_w_proportion_int[id];
				id = (id + 1) % size;
			}
		}

		for (int k = 0; k < size; ++k) {
			int32_t* p = memory + 2 * k;
			*p = k; ++p;
			*p = (k + 1) * height;
		}

		int32_t L_head = 0, L_tail = 0;
		int32_t H_head = 0, H_tail = 0;

		for (int k = 0; k < size; ++k) {
			auto val = q_w_proportion_int[k];
			if (val < height) {
				L[L_tail].first = k;
				L[L_tail].second = val;
				++L_tail;
			}
			else {
				H[H_tail].first = k;
				H[H_tail].second = val;
				++H_tail;
			}
		}

		CHECK(L_tail + H_tail == size);

		while (L_head != L_tail && H_head != H_tail) {
			auto& l_pl = L[L_head++];
			auto& h_ph = H[H_head++];

			int32_t *p = memory + 2 * l_pl.first;
			*p = h_ph.first; ++p;
			*p = l_pl.first * height + l_pl.second;

			auto sum = h_ph.second + l_pl.second;
			if (sum > 2 * height) {
				H[H_tail].first = h_ph.first;
				H[H_tail].second = sum - height;
				++H_tail;
			}
			else {
				L[L_tail].first = h_ph.first;
				L[L_tail].second = sum - height;
				++L_tail;
			}
		}
		while (L_head != L_tail) {
			auto first = L[L_head].first;
			auto second = L[L_head].second;
			int32_t* p = memory + 2 * first;
			*p = first; ++p;
			*p = first * height + second;
			++L_head;
		}
		while (H_head != H_tail) {
			auto first = H[H_head].first;
			auto second = H[H_head].second;
			int32_t* p = memory + 2 * first;
			*p = first; ++p;
			*p = first * height + second;
			++H_head;
		}
	}

	void AliasSlice::GenerateBetaAliasRow(petuum::ClientSummaryRow& summary_row, wood::xorshift_rng& rng) {
		real_t beta_mass = 0;
		std::vector<real_t>& q_w_proportion = *q_w_proportion_;
		std::vector<int32_t>& q_w_proportion_int = *q_w_proportion_int_;

		std::vector<std::pair<int32_t, int32_t>>& L = *L_;
		std::vector<std::pair<int32_t, int32_t>>& H = *H_;

		for (int k = 0; k < K_; ++k) {
			q_w_proportion[k] = beta_ / (summary_row.GetSummaryCount(k) + beta_sum_);
			beta_mass += q_w_proportion[k];
		}
		beta_mass_ = beta_mass;
		int32_t mass_int = 0x7FFFFFFF;

		beta_height_ = mass_int / K_;
		mass_int = beta_height_ * K_;

		int64_t mass_sum = 0;
		for (int k = 0; k < K_; ++k) {
			q_w_proportion[k] /= beta_mass;
			q_w_proportion_int[k] = q_w_proportion[k] * mass_int;
			mass_sum += q_w_proportion_int[k];
		}

		if (mass_sum > mass_int) {
			int32_t more = mass_sum - mass_int;
			int id = 0;
			for (int i = 0; i < more;) {
				// int id = rng.rand_k(size);
				if (q_w_proportion_int[id] >= 1) {
					--q_w_proportion_int[id];
					++i;
				}
				id = (id + 1) % K_;
			}
		}

		if (mass_sum < mass_int) {
			int32_t more = mass_int - mass_sum;
			int id = 0;
			for (int i = 0; i < more; ++i) {
				// int id = rng.rand_k(size);
				++q_w_proportion_int[id];
				id = (id + 1) % K_;
			}
		}
		
		for (int k = 0; k < K_; ++k) {
			beta_k_[k] = k;
			beta_v_[k] = (k + 1) * beta_height_;
		}

		int32_t L_head = 0, L_tail = 0;
		int32_t H_head = 0, H_tail = 0;

		for (int k = 0; k < K_; ++k) {
			auto val = q_w_proportion_int[k];
			if (val < beta_height_) {
				L[L_tail].first = k;
				L[L_tail].second = val;
				++L_tail;
			}
			else {
				H[H_tail].first = k;
				H[H_tail].second = val;
				++H_tail;
			}
		}

		CHECK(L_tail + H_tail == K_);

		while (L_head != L_tail && H_head != H_tail) {
			auto& l_pl = L[L_head++];
			auto& h_ph = H[H_head++];

			beta_k_[l_pl.first] = h_ph.first;
			beta_v_[l_pl.first] = l_pl.first * beta_height_ + l_pl.second;

			auto sum = h_ph.second + l_pl.second;
			if (sum > 2 * beta_height_) {
				H[H_tail].first = h_ph.first;
				H[H_tail].second = sum - beta_height_;
				++H_tail;
			}
			else {
				L[L_tail].first = h_ph.first;
				L[L_tail].second = sum - beta_height_;
				++L_tail;
			}
		}

		while (L_head != L_tail) {
			auto first = L[L_head].first;
			auto second = L[L_head].second;
			beta_k_[first] = first;
			beta_v_[first] = first * beta_height_ + second;
			++L_head;
		}
		while (H_head != H_tail) {
			auto first = H[H_head].first;
			auto second = H[H_head].second;
			beta_k_[first] = first;
			beta_v_[first] = first * beta_height_ + second;
			++H_head;
		}
	}
}