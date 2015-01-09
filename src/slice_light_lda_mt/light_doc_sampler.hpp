// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date  : 2014.8.2

#pragma once

#include "common.h"
#include "lda_document.hpp"
#include "rand_int_rng.h"
#include <glog/logging.h>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>
#include <queue>
#include <map>
//#include <sparsehash/dense_hash_map>
#include "alias_multinomial_rng_int.hpp"
#include "light_hash_map.h"
#include "data_block.h"
#include "utils.hpp"
#include "hybrid_map.h"

namespace lda
{
	struct word_topic_delta
	{
		int32_t word;
		int32_t topic;
		int32_t delta;
	};

	class LightDocSampler
	{
	public:
		LightDocSampler(
			std::vector<lda::hybrid_map> &word_topic_table,
			std::vector<int64_t> &summary_row,
			std::vector<lda::hybrid_alias_map> &alias_kv,
			int32_t beta_height,
			float beta_mass,
			std::vector<wood::alias_k_v> beta_k_v
			);

		~LightDocSampler();

		int32_t GlobalInit(LDADocument *doc);

		int32_t SampleOneDoc(LDADocument *doc, int32_t slice_upper);


		// The i-th complete-llh calculation will use row i in llh_able_. This is
		// part of log P(z) in eq.[3].
		double ComputeOneDocLLH(LDADocument* doc);
		double ComputeWordLLH(int32_t lower, int32_t upper);
		double NormalizeWordLLH()
		{
			double word_llh = K_ * log_topic_normalizer_;
			for (int k = 0; k < K_; ++k)
			{
				word_llh -= LogGamma(summary_row_[k] + beta_sum_);
				CHECK_EQ(word_llh, word_llh)
					<< "word_llh is nan after -LogGamma(summary_row[k] + beta_). "
					<< "summary_row[k] = " << summary_row_[k];
			}
			return word_llh;
		}

		void Dump(const std::string &dump_name, int32_t lower, int32_t upper);


		int32_t DocInit(LDADocument *doc);
		/*
		void EpocInit(int32_t lower, int32_t upper)
		{
			for (int w = lower; w < upper; ++w)
			{
				GenerateAliasTableforWord(w);
			}

			std::fill(delta_summary_row_.begin(), delta_summary_row_.end(), 0);
			for (auto &shard : word_topic_delta_)
			{
				shard.clear();
			}
		}
		*/

		void build_alias_table(int32_t lower, int32_t upper)
		{
			//for (int i = 0; i < 10; ++i)
			{
				for (int w = lower; w < upper; ++w)
				{
					GenerateAliasTableforWord(w);
				}
			}
		}
		void EpocInit()
		{
			std::fill(delta_summary_row_.begin(), delta_summary_row_.end(), 0);
			for (auto &shard : word_topic_delta_)
			{
				shard.clear();
			}
		}

		void build_word_topic_table(int32_t thread_id, int32_t num_threads, lda::LDAModelBlock &model_block)
		{
			for (int i = 0; i < V_; ++i)
			{
				if (i % num_threads == thread_id)
				{
					word_topic_table_[i] = model_block.get_row(i, rehashing_buf_);
				}
			}
		}


		inline int32_t rand_k()
		{
			return rng_.rand_k(K_);
		}

		inline lda::hybrid_map& get_word_row(int32_t word)
		{
			return word_topic_table_[word];
		}

		inline std::vector<int64_t> &get_summary_row()
		{
			return summary_row_;
		}
		inline void zero_statistics()
		{
			num_sampling_ = 0;
			num_sampling_changed_ = 0;
		}
		inline void print_statistics()
		{
			LOG(INFO) << "num_sampling = " << num_sampling_ << ", num_sampling_changed = " << num_sampling_changed_;
		}
		inline std::vector<word_topic_delta>& get_word_topic_delta(int32_t thread_id)
		{
			return word_topic_delta_[thread_id];
		}
		inline std::vector<int64_t>& get_delta_summary_row()
		{
			return delta_summary_row_;
		}

	private:
		inline int32_t Sample2WordFirst(LDADocument *doc, int32_t w, int32_t s, int32_t old_topic);

		inline void GenerateAliasTableforWord(int32_t word);

		inline int32_t get_word_topic(int32_t word, int32_t topic);
		inline void word_topic_dec(int32_t word, int32_t topic);
		inline void word_topic_inc(int32_t word, int32_t topic);

		int32_t OldProposalFreshSample(LDADocument *doc, int32_t slice_upper);

	private:
		int32_t num_sampling_;
		int32_t num_sampling_changed_;

		int32_t num_tokens_;
		int32_t num_unique_words_;

		int32_t K_;
		int32_t V_;
		real_t beta_;
		real_t beta_sum_;
		real_t alpha_;
		real_t alpha_sum_;

		real_t ll_alpha_;
		real_t ll_alpha_sum_;

		real_t delta_alpha_sum_;
		//int32_t decay_step_;

		std::vector<float> q_w_proportion_;
		wood::AliasMultinomialRNGInt alias_rng_;
		wood::xorshift_rng rng_;
		std::vector<lda::hybrid_alias_map> &alias_k_v_;

		int32_t doc_size_;

		// the number of Metropolis Hastings step
		int32_t mh_step_for_gs_;
		real_t n_td_sum_;


		// model
		std::vector<int64_t> &summary_row_;
		std::vector<lda::hybrid_map> &word_topic_table_;
		int32_t *rehashing_buf_;

		int32_t &beta_height_;
		float &beta_mass_;
		std::vector<wood::alias_k_v> &beta_k_v_;

		// delta
		std::vector<int64_t> delta_summary_row_;

		int32_t num_threads_;
		std::vector<std::vector<word_topic_delta>> word_topic_delta_;	

		// ================ Precompute LLH Parameters =================
		// Log of normalization constant (per docoument) from eq.[3].
		double log_doc_normalizer_;

		// Log of normalization constant (per topic) from eq.[2].
		double log_topic_normalizer_;

		wood::light_hash_map doc_topic_counter_;
	};
	

	inline int32_t LightDocSampler::get_word_topic(int32_t word, int32_t topic)
	{
		return word_topic_table_[word][topic];
	}	
	inline void LightDocSampler::word_topic_dec(int32_t word, int32_t topic)
	{
		word_topic_table_[word].inc(topic, -1);
	}
	inline void LightDocSampler::word_topic_inc(int32_t word, int32_t topic)
	{
		word_topic_table_[word].inc(topic, 1);
	}
	inline void LightDocSampler::GenerateAliasTableforWord(int32_t word)
	{
		//LOG(INFO) << "word = " << word;
		alias_k_v_[word].build_table(alias_rng_, word_topic_table_[word], summary_row_, q_w_proportion_, beta_, beta_sum_);
	}

	inline int32_t LightDocSampler::Sample2WordFirst(LDADocument *doc, int32_t w, int32_t s, int32_t old_topic)
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
			// n_tw proposal

			t = alias_k_v_[w].next(rng_, beta_height_, beta_mass_, beta_k_v_);

			rejection = rng_.rand_real();

			n_td_alpha = doc_topic_counter_[t] + alpha_;
			n_sd_alpha = doc_topic_counter_[s] + alpha_;

			if (s != old_topic && t != old_topic)
			{
				w_t_cnt = get_word_topic(w, t);
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_;

				w_s_cnt = get_word_topic(w, s);
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_;
			}
			else if (s != old_topic && t == old_topic)
			{
				n_td_alpha -= 1;

				w_t_cnt = get_word_topic(w, t) - 1;
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_ - 1;

				w_s_cnt = get_word_topic(w, s);
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_;
			}
			else if (s == old_topic && t != old_topic)
			{
				n_sd_alpha -= 1;

				w_t_cnt = get_word_topic(w, t);
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_;

				w_s_cnt = get_word_topic(w, s) - 1;
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_ - 1;
			}
			else
			{
				n_td_alpha -= 1;
				n_sd_alpha -= 1;

				w_t_cnt = get_word_topic(w, t) - 1;
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_ - 1;

				w_s_cnt = get_word_topic(w, s) - 1;
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_ - 1;
			}

			w_s_cnt = get_word_topic(w, s);
			w_t_cnt = get_word_topic(w, t);
			proposal_s = (w_s_cnt + beta_) / (summary_row_[s] + beta_sum_);
			proposal_t = (w_t_cnt + beta_) / (summary_row_[t] + beta_sum_);

			nominator = n_td_alpha
				* n_tw_beta
				* n_s_beta_sum
				* proposal_s;

			denominator = n_sd_alpha
				* n_sw_beta
				* n_t_beta_sum
				* proposal_t;


			pi = std::min((real_t)1.0, nominator / denominator);

			// s = rejection < pi ? t : s;
			m = -(rejection < pi);
			s = (t & m) | (s & ~m);

			real_t n_td_or_alpha = rng_.rand_real() * (n_td_sum_ + alpha_sum_);
			if (n_td_or_alpha < n_td_sum_)
			{
				int32_t t_idx = rng_.rand_k(doc_size_);
				t = doc->Topic(t_idx);
			}
			else
			{
				t = rng_.rand_k(K_);
			}

			rejection = rng_.rand_real();

			n_td_alpha = doc_topic_counter_[t] + alpha_;
			n_sd_alpha = doc_topic_counter_[s] + alpha_;


			if (s != old_topic && t != old_topic)
			{
				w_t_cnt = get_word_topic(w, t);
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_;

				w_s_cnt = get_word_topic(w, s);
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_;
			}
			else if (s != old_topic && t == old_topic)
			{
				n_td_alpha -= 1;

				w_t_cnt = get_word_topic(w, t) - 1;
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_ - 1;

				w_s_cnt = get_word_topic(w, s);
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_;
			}
			else if (s == old_topic && t != old_topic)
			{
				n_sd_alpha -= 1;

				w_t_cnt = get_word_topic(w, t);
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_;

				w_s_cnt = get_word_topic(w, s) - 1;
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_ - 1;
			}
			else
			{
				//TODO(jiyuan): s == t, can be simplified
				n_td_alpha -= 1;
				n_sd_alpha -= 1;

				w_t_cnt = get_word_topic(w, t) - 1;
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_ - 1;

				w_s_cnt = get_word_topic(w, s) - 1;
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_ - 1;
			}

			proposal_t = doc_topic_counter_[t] + alpha_;
			proposal_s = doc_topic_counter_[s] + alpha_;

			nominator = n_td_alpha
				* n_tw_beta
				* n_s_beta_sum
				* proposal_s;

			denominator = n_sd_alpha
				* n_sw_beta
				* n_t_beta_sum
				* proposal_t;


			pi = std::min((real_t)1.0, nominator / denominator);

			// s = rejection < pi ? t : s;
			m = -(rejection < pi);
			s = (t & m) | (s & ~m);
		}
		int32_t src = s;
		return src;
	}	
}