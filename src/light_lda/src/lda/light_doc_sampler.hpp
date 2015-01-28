// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date  : 2014.8.2

#pragma once

#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <glog/logging.h>
#include "base/common.hpp"
#include "memory/alias_slice.h"
#include "memory/data_block.h"
#include "memory/model_slice.h"
#include "memory/summary_row.hpp"
#include "util/delta_table.h"
#include "util/light_hash_map.h"
#include "util/rand_int_rng.h"

namespace lda
{
	class LightDocSampler
	{
	public:
		LightDocSampler();
		~LightDocSampler();

		// return value: num of words sampled in current model slices
		int32_t SampleOneDoc(LDADocument *doc, ModelSlice& word_topic_table,
			petuum::ClientSummaryRow& summary_row, AliasSlice& alias_table,
			std::vector<std::unique_ptr<petuum::DeltaArray>>& word_topic_delta_vec, 
			petuum::SummaryDelta& summary_delta);

		void InferOneDoc(LDADocument* doc, ModelSlice& word_topic_table,
			petuum::ClientSummaryRow& summary_row, AliasSlice& alias_table);

		int32_t DocInit(LDADocument *doc);

		inline void zero_statistics()
		{
			num_sampling_ = 0;
			num_sampling_changed_ = 0;
			num_accept_ = 0;
			num_total_ = 0;
		}

		inline void print_statistics()
		{
			LOG(INFO) << "num_sampling = " << num_sampling_ << ", num_sampling_changed = " << num_sampling_changed_;
			LOG(INFO) << "accept ratio = " << static_cast<double>(num_accept_) / num_total_;
		}

		wood::xorshift_rng& rng() {
			return rng_;
		}

	private:

		inline int32_t Sample2WordFirst(LDADocument *doc, int32_t w, int32_t s, int32_t old_topic,
			ModelSlice& word_topic_table,
			petuum::ClientSummaryRow& summary_row,
			AliasSlice& alias_table);

		inline int32_t InferWordFirst(LDADocument* doc, int32_t w, int32_t s, int32_t old_topic,
			ModelSlice& word_topic_table, petuum::ClientSummaryRow& summary_row, AliasSlice& alias_table);

	private:
		int gs_type_;

		int32_t num_sampling_;
		int32_t num_sampling_changed_;

		int32_t num_tokens_;
		int32_t num_unique_words_;

		int32_t K_;
		int32_t V_;
		real_t beta_;
		real_t beta_sum_;
		real_t alpha_;

		int32_t num_accept_;
		int32_t num_total_;

		wood::light_hash_map doc_topic_counter_;
		int32_t doc_size_;

		// the number of Metropolis Hastings step
		int32_t mh_step_for_gs_;

		real_t n_td_sum_;
		real_t alpha_sum_;

		wood::xorshift_rng rng_;
	};


	inline int32_t LightDocSampler::Sample2WordFirst(
		LDADocument *doc, int32_t w, int32_t s, int32_t old_topic,
		ModelSlice& word_topic_table,
		petuum::ClientSummaryRow& summary_row,
		AliasSlice& alias_table)
	{
		int32_t w_t_cnt;
		int32_t w_s_cnt;

		real_t n_td_alpha;
		real_t n_sd_alpha;
		real_t n_tw_beta;
		real_t n_sw_beta;
		real_t n_s_beta_sum;
		real_t n_t_beta_sum;

		real_t proposal_s;
		real_t proposal_t;

		real_t nominator;
		real_t denominator;

		real_t rejection;
		real_t pi;
		int m;

		for (int i = 0; i < mh_step_for_gs_; ++i)
		{
			int32_t t;

			// word proposal
			
			t = alias_table.ProposeTopic(w, rng_);
			rejection = rng_.rand_double();

			w_t_cnt = word_topic_table.GetWordTopicCount(w, t); 
			w_s_cnt = word_topic_table.GetWordTopicCount(w, s);

			if (s != old_topic && t != old_topic)
			{
				n_td_alpha = doc_topic_counter_[t] + alpha_;
				n_sd_alpha = doc_topic_counter_[s] + alpha_;

				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row.GetSummaryCount(t) + beta_sum_;

				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row.GetSummaryCount(s) + beta_sum_;
			}
			else if (s != old_topic && t == old_topic)
			{
				n_td_alpha = doc_topic_counter_[t] + alpha_ - 1;
				n_sd_alpha = doc_topic_counter_[s] + alpha_;

				n_tw_beta = w_t_cnt - 1 + beta_;
				n_t_beta_sum = summary_row.GetSummaryCount(t) + beta_sum_ - 1;

				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row.GetSummaryCount(s) + beta_sum_;
			}
			else if (s == old_topic && t != old_topic)
			{
				n_td_alpha = doc_topic_counter_[t] + alpha_;
				n_sd_alpha = doc_topic_counter_[s] + alpha_ - 1;

				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row.GetSummaryCount(t) + beta_sum_;

				n_sw_beta = w_s_cnt - 1 + beta_;
				n_s_beta_sum = summary_row.GetSummaryCount(s) + beta_sum_ - 1;
			}
			else
			{
				//TODO(jiyuan): s == t, can be simplified
				n_td_alpha = doc_topic_counter_[t] + alpha_ - 1;
				n_sd_alpha = doc_topic_counter_[s] + alpha_ - 1;

				n_tw_beta = w_t_cnt - 1 + beta_;
				n_t_beta_sum = summary_row.GetSummaryCount(t) + beta_sum_ - 1;

				n_sw_beta = w_s_cnt - 1 + beta_;
				n_s_beta_sum = summary_row.GetSummaryCount(s) + beta_sum_ - 1;
			}

			proposal_s = (w_s_cnt + beta_) / 
				(summary_row.GetSummaryCount(s) + beta_sum_); 
			proposal_t = (w_t_cnt + beta_) / 
				(summary_row.GetSummaryCount(t) + beta_sum_); 

			nominator = n_td_alpha
				* n_tw_beta
				* n_s_beta_sum
				* proposal_s;

			denominator = n_sd_alpha
				* n_sw_beta
				* n_t_beta_sum
				* proposal_t;

			pi = (std::min)((real_t)1.0, nominator / denominator);

			m = -(rejection < pi);
			s = (t & m) | (s & ~m);
			num_accept_ += (rejection < pi) ? 1 : 0;
			num_total_ += 1;

			// doc_proposal
			
			real_t n_td_or_alpha = rng_.rand_double() * (n_td_sum_ + alpha_sum_);
			if (n_td_or_alpha < n_td_sum_)
			{
				int32_t t_idx = rng_.rand_k(doc_size_); 
				t = doc->Topic(t_idx);
			}
			else
			{
				t = rng_.rand_k(K_);
			}

			rejection = rng_.rand_double();

			w_t_cnt = 0; w_s_cnt = 0;
			
			w_t_cnt = word_topic_table.GetWordTopicCount(w, t);
			w_s_cnt = word_topic_table.GetWordTopicCount(w, s);


			if (s != old_topic && t != old_topic)
			{
				n_td_alpha = doc_topic_counter_[t] + alpha_;
				n_sd_alpha = doc_topic_counter_[s] + alpha_;

				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row.GetSummaryCount(t) + beta_sum_;

				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row.GetSummaryCount(s) + beta_sum_;
			}
			else if (s != old_topic && t == old_topic)
			{
				n_td_alpha = doc_topic_counter_[t] + alpha_ - 1;
				n_sd_alpha = doc_topic_counter_[s] + alpha_;

				n_tw_beta = w_t_cnt - 1 + beta_;
				n_t_beta_sum = summary_row.GetSummaryCount(t) + beta_sum_ - 1;

				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row.GetSummaryCount(s) + beta_sum_;
			}
			else if (s == old_topic && t != old_topic)
			{
				n_td_alpha = doc_topic_counter_[t] + alpha_;
				n_sd_alpha = doc_topic_counter_[s] + alpha_ - 1;

				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row.GetSummaryCount(t) + beta_sum_;

				n_sw_beta = w_s_cnt - 1 + beta_;
				n_s_beta_sum = summary_row.GetSummaryCount(s) + beta_sum_ - 1;
			}
			else
			{
				//TODO(jiyuan): s == t, can be simplified
				n_td_alpha = doc_topic_counter_[t] + alpha_ - 1;
				n_sd_alpha = doc_topic_counter_[s] + alpha_ - 1;

				n_tw_beta = w_t_cnt - 1 + beta_;
				n_t_beta_sum = summary_row.GetSummaryCount(t) + beta_sum_ - 1;

				n_sw_beta = w_s_cnt - 1 + beta_;
				n_s_beta_sum = summary_row.GetSummaryCount(s) + beta_sum_ - 1;
			}

			proposal_s = (doc_topic_counter_[s] + alpha_);
			proposal_t = (doc_topic_counter_[t] + alpha_);

			nominator = n_td_alpha
				* n_tw_beta
				* n_s_beta_sum
				* proposal_s;

			denominator = n_sd_alpha
				* n_sw_beta
				* n_t_beta_sum
				* proposal_t;
			
			pi = (std::min)((real_t)1.0, nominator / denominator);

			// s = rejection < pi ? t : s;
			m = -(rejection < pi);
			s = (t & m) | (s & ~m);
			num_accept_ += (rejection < pi) ? 1 : 0;
			num_total_ += 1;
		}
		
		int32_t src = s;
		return src;
	}

	inline int32_t LightDocSampler::InferWordFirst(
		LDADocument* doc, int32_t w, int32_t s, int32_t old_topic,
		ModelSlice& word_topic_table, petuum::ClientSummaryRow& summary_row, AliasSlice& alias_table) 
	{
		int32_t w_t_cnt;
		int32_t w_s_cnt;

		real_t n_td_alpha;
		real_t n_sd_alpha;
		real_t n_tw_beta;
		real_t n_sw_beta;
		real_t n_s_beta_sum;
		real_t n_t_beta_sum;

		real_t proposal_s;
		real_t proposal_t;

		real_t nominator;
		real_t denominator;

		real_t rejection;
		real_t pi;
		int m;

		for (int i = 0; i < mh_step_for_gs_; ++i)
		{
			int32_t t;

			// word proposal

			t = alias_table.ProposeTopic(w, rng_);
			rejection = rng_.rand_double();

			n_td_alpha = doc_topic_counter_[t] + alpha_;
			n_sd_alpha = doc_topic_counter_[s] + alpha_;

			w_t_cnt = word_topic_table.GetWordTopicCount(w, t);
			w_s_cnt = word_topic_table.GetWordTopicCount(w, s);

			nominator = n_td_alpha;

			denominator = n_sd_alpha;

			pi = (std::min)((real_t)1.0, nominator / denominator);

			m = -(rejection < pi);
			s = (t & m) | (s & ~m);

			// doc_proposal

			real_t n_td_or_alpha = rng_.rand_double() * (n_td_sum_ + alpha_sum_);
			if (n_td_or_alpha < n_td_sum_)
			{
				int32_t t_idx = rng_.rand_k(doc_size_);
				t = doc->Topic(t_idx);
			}
			else
			{
				t = rng_.rand_k(K_);
			}

			rejection = rng_.rand_double();

			w_t_cnt = 0; w_s_cnt = 0;

			w_t_cnt = word_topic_table.GetWordTopicCount(w, t);
			w_s_cnt = word_topic_table.GetWordTopicCount(w, s);

			n_tw_beta = w_t_cnt + beta_;
			n_t_beta_sum = summary_row.GetSummaryCount(t) + beta_sum_;

			n_sw_beta = w_s_cnt + beta_;
			n_s_beta_sum = summary_row.GetSummaryCount(s) + beta_sum_;

			nominator = n_tw_beta * n_s_beta_sum;

			denominator = n_sw_beta * n_t_beta_sum;

			pi = (std::min)((real_t)1.0, nominator / denominator);

			// s = rejection < pi ? t : s;
			m = -(rejection < pi);
			s = (t & m) | (s & ~m);
		}

		int32_t src = s;
		return src;
	}
}