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
#include <sparsehash/dense_hash_map>
#include "alias_multinomial_rng.hpp"

//#define USE_HASH
#ifdef USE_HASH
#define GOOGLE_HASH
#endif

namespace lda
{
#ifdef GOOGLE_HASH
	typedef google::dense_hash_map<int32_t, int32_t> dense_hash_row_t;
	//typedef google::dense_hash_map<int32_t, dense_hash_row_t> dense_hash_table_t;
	typedef std::vector<dense_hash_row_t> dense_hash_table_t;
	typedef google::dense_hash_map<int32_t, dense_hash_row_t> dense_oplog_t;
#else
	typedef std::unordered_map<int32_t, int32_t> dense_hash_row_t;
	typedef std::vector<dense_hash_row_t> dense_hash_table_t;
	typedef std::unordered_map<int32_t, dense_hash_row_t> dense_oplog_t;
#endif
	class LightDocSampler
	{
	public:
		LightDocSampler();
		~LightDocSampler();

		int32_t GlobalInit(DocBatch &docs);

		void SampleOneDoc(Document *doc);
		//void SampleOneDoc2(LDADocument *doc);

		void UpdateAlpha(int32_t iter)
		{
			/*
			alpha_ = 0.1;
			alpha_sum_ = alpha_ * K_;
			
			alpha_sum_ -= delta_alpha_sum_;
			alpha_sum_ = alpha_sum_ < 0 ? 0 : alpha_sum_;
			
			*/

			if (iter < 10)
			{
				alpha_ = 0.03;
			}
			else if (iter < 20)
			{
				alpha_ = 0.02;
			}
			else
			{
				alpha_ = 0.01;
			}

			alpha_sum_ = alpha_ * K_;

			LOG(INFO) << "alpha_sum = " << alpha_sum_;
			
		}

		// The i-th complete-llh calculation will use row i in llh_able_. This is
		// part of log P(z) in eq.[3].
		double ComputeOneDocLLH(Document* doc);
		double ComputeWordLLH();

		void Dump(const std::string &dump_name);


		int32_t DocInit(Document *doc);

		void EpocInit()
		{
			for (int w = 0; w < V_; ++w)
			{
				GenerateAliasTableforWord(w);
			}
		}

#ifdef USE_HASH
		inline dense_hash_row_t& get_word_row(int32_t word)
		{
			return word_topic_hash_table_[word];
		}

#else
		inline std::vector<int32_t> &get_word_row(int32_t word)
		{
			return word_topic_table_[word];
		}		

#endif
		inline std::vector<int32_t> &get_summary_row()
		{
			return summary_row_;
		}
		inline void zero_statistics()
		{
			num_sampling_ = 0;
			num_sampling_changed_ = 0;
			oplog_.clear();
		}

		inline void print_statistics()
		{
			LOG(INFO) << "num_sampling = " << num_sampling_ << ", num_sampling_changed = " << num_sampling_changed_;
		}

	private:

		inline int32_t SampleWordFirst(Document *doc, int32_t w, int32_t s);
		inline int32_t Sample2WordFirst(Document *doc, int32_t w, int32_t s, int32_t old_topic);

		//inline int32_t SampleWordFirst2(LDADocument *doc, int32_t w, int32_t s, int32_t old_topic);

		inline void GenerateAliasTableforWord(int32_t word);


		inline int32_t get_word_topic(int32_t word, int32_t topic);
		inline void word_topic_dec(int32_t word, int32_t topic);
		inline void word_topic_inc(int32_t word, int32_t topic);

		inline void oplog_delta(int32_t word, int32_t old_topic, int32_t new_topic)
		{
			// ORIG: auto &it = oplog_.find(word);
			const auto &it = oplog_.find(word);
			if (it == oplog_.end())
			{
				oplog_[word];
#ifdef GOOGLE_HASH
				oplog_[word].set_empty_key(-1);
				oplog_[word].set_deleted_key(-1);
#endif
			}
			oplog_[word][old_topic] -= 1;
			oplog_[word][new_topic] += 1;

		}

		void OldProposalFreshSample(Document *doc);
		void OldProposalOldSample(Document *doc);

		void OldProposalFreshSample2(Document *doc);

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
		real_t alpha_sum_;

		real_t ll_alpha_;
		real_t ll_alpha_sum_;

		real_t delta_alpha_sum_;
		int32_t decay_step_;

		wood::AliasMultinomialRNG alias_rng_;
		std::vector<real_t> q_w_sum_;
		std::vector<std::vector<real_t>> q_w_proportion_;
		std::vector<std::vector<int32_t>> s_w_;
		std::vector<int32_t> s_w_num_;



		std::vector<int32_t> doc_topic_vec_;
		
		int32_t doc_size_;

		// the number of Metropolis Hastings step
		int32_t mh_step_for_gs_;

		real_t n_td_sum_;


		// model
		std::vector<int32_t> summary_row_;
		std::vector<std::vector<int32_t>> word_topic_table_;
		
		dense_hash_table_t word_topic_hash_table_;
		dense_oplog_t oplog_;


		int32_t cache_count_;
		int32_t cache_size_;	


		// ================ Precompute LLH Parameters =================
		// Log of normalization constant (per docoument) from eq.[3].
		double log_doc_normalizer_;

		// Log of normalization constant (per topic) from eq.[2].
		double log_topic_normalizer_;
	};

	


	inline int32_t LightDocSampler::get_word_topic(int32_t word, int32_t topic)
	{
#ifdef USE_HASH
		auto topic_iter = word_topic_hash_table_[word].find(topic);
		if (topic_iter != word_topic_hash_table_[word].end())
		{
			return topic_iter->second;
		}
		return 0;
#else
		return word_topic_table_[word][topic];
#endif
	}
	
	
	inline void LightDocSampler::word_topic_dec(int32_t word, int32_t topic)
	{
#ifdef USE_HASH
		word_topic_hash_table_[word][topic] -= 1;
		if (word_topic_hash_table_[word][topic] == 0)
		{
			auto topic_iter = word_topic_hash_table_[word].find(topic);
			word_topic_hash_table_[word].erase(topic_iter);
		}
#else
		--word_topic_table_[word][topic];
#endif
	}

	inline void LightDocSampler::word_topic_inc(int32_t word, int32_t topic)
	{
#ifdef USE_HASH
		word_topic_hash_table_[word][topic] += 1;
#else
		++word_topic_table_[word][topic];
#endif
	}


	inline void LightDocSampler::GenerateAliasTableforWord(int32_t word)
	{
		// update q_w_sum_[word], q_w_proportion_[word] and s_w_[w];
		real_t q_w_sum = 0.;
		std::vector<real_t> q_w_proportion(K_);
		for (int k = 0; k < K_; ++k)
		{
			int32_t n_tw = get_word_topic(word, k);
			real_t prop = (n_tw + beta_) / (summary_row_[k] + beta_sum_);
			q_w_proportion[k] = prop;
			q_w_sum += prop;
		}
		q_w_sum_[word] = q_w_sum;
		q_w_proportion_[word] = q_w_proportion;
		alias_rng_.SetProportionMass(q_w_proportion, q_w_sum);
		for (int i = 0; i < K_; ++i)
		{
			s_w_[word][i] = alias_rng_.Next();
		}
		s_w_num_[word] = K_ - 1;
	}

	
	inline int32_t LightDocSampler::SampleWordFirst(Document *doc, int32_t w, int32_t s)
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

		std::vector<int32_t> &word_topics = doc->get_topics();

		for (int i = 0; i < mh_step_for_gs_; ++i)
		{
			// n_td proposal
			int32_t t;
			// n_tw proposal
			t = s_w_[w][s_w_num_[w]];
			--s_w_num_[w];

			rejection = wood::fast_rand_double();

			w_t_cnt = get_word_topic(w, t);
			n_tw_beta = w_t_cnt + beta_;
			n_t_beta_sum = summary_row_[t] + beta_sum_;

			w_s_cnt = get_word_topic(w, s);
			n_sw_beta = w_s_cnt + beta_;
			n_s_beta_sum = summary_row_[s] + beta_sum_;

			nominator = n_tw_beta * n_s_beta_sum;
			denominator = n_sw_beta * n_t_beta_sum;

			pi = std::min((real_t)1.0, nominator / denominator);

			// s = rejection < pi ? t : s;
			m = -(rejection < pi);
			s = (t & m) | (s & ~m);

			real_t n_td_or_alpha = wood::fast_rand_double() * (n_td_sum_ + alpha_sum_);
			if (n_td_or_alpha < n_td_sum_)
			{
				int32_t t_idx = wood::intel_fast_k(doc_size_);
				t = word_topics[t_idx];
			}
			else
			{
				t = wood::intel_fast_k(K_);
			}
			rejection = wood::fast_rand_double();

			w_t_cnt = get_word_topic(w, t);
			n_tw_beta = w_t_cnt + beta_;
			n_t_beta_sum = summary_row_[t] + beta_sum_;

			w_s_cnt = get_word_topic(w, s);
			n_sw_beta = w_s_cnt + beta_;
			n_s_beta_sum = summary_row_[s] + beta_sum_;

			nominator = n_tw_beta * n_s_beta_sum;
			denominator = n_sw_beta * n_t_beta_sum;

			pi = std::min((real_t)1.0, nominator / denominator);

			// s = rejection < pi ? t : s;
			m = -(rejection < pi);
			s = (t & m) | (s & ~m);
		}
		int32_t src = s;
		return src;
	}

	inline int32_t LightDocSampler::Sample2WordFirst(Document *doc, int32_t w, int32_t s, int32_t old_topic)
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


		std::vector<int32_t> &word_topics = doc->get_topics();

		for (int i = 0; i < mh_step_for_gs_; ++i)
		{
			// n_td proposal
			int32_t t;


			// n_tw proposal
			t = s_w_[w][s_w_num_[w]];
			--s_w_num_[w];

			rejection = wood::fast_rand_double();

			if (s != old_topic && t != old_topic)
			{
				n_td_alpha = doc_topic_vec_[t] + alpha_;
				n_sd_alpha = doc_topic_vec_[s] + alpha_;

				w_t_cnt = get_word_topic(w, t);
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_;

				w_s_cnt = get_word_topic(w, s);
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_;
			}
			else if (s != old_topic && t == old_topic)
			{
				n_td_alpha = doc_topic_vec_[t] + alpha_ - 1;
				n_sd_alpha = doc_topic_vec_[s] + alpha_;

				w_t_cnt = get_word_topic(w, t) - 1;
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_ - 1;

				w_s_cnt = get_word_topic(w, s);
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_;
			}
			else if (s == old_topic && t != old_topic)
			{
				n_td_alpha = doc_topic_vec_[t] + alpha_;
				n_sd_alpha = doc_topic_vec_[s] + alpha_ - 1;

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
				n_td_alpha = doc_topic_vec_[t] + alpha_ - 1;
				n_sd_alpha = doc_topic_vec_[s] + alpha_ - 1;

				w_t_cnt = get_word_topic(w, t) - 1;
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_ - 1;

				w_s_cnt = get_word_topic(w, s) - 1;
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_ - 1;
			}

			proposal_s = q_w_proportion_[w][s];
			proposal_t = q_w_proportion_[w][t];

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

			real_t n_td_or_alpha = wood::fast_rand_double() * (n_td_sum_ + alpha_sum_);
			if (n_td_or_alpha < n_td_sum_)
			{
				int32_t t_idx = wood::intel_fast_k(doc_size_);
				t = word_topics[t_idx];
			}
			else
			{
				t = wood::intel_fast_k(K_);
			}

			rejection = wood::fast_rand_double();

			if (s != old_topic && t != old_topic)
			{
				n_td_alpha = doc_topic_vec_[t] + alpha_;
				n_sd_alpha = doc_topic_vec_[s] + alpha_;

				w_t_cnt = get_word_topic(w, t);
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_;

				w_s_cnt = get_word_topic(w, s);
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_;
			}
			else if (s != old_topic && t == old_topic)
			{
				n_td_alpha = doc_topic_vec_[t] + alpha_ - 1;
				n_sd_alpha = doc_topic_vec_[s] + alpha_;

				w_t_cnt = get_word_topic(w, t) - 1;
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_ - 1;

				w_s_cnt = get_word_topic(w, s);
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_;
			}
			else if (s == old_topic && t != old_topic)
			{
				n_td_alpha = doc_topic_vec_[t] + alpha_;
				n_sd_alpha = doc_topic_vec_[s] + alpha_ - 1;

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
				n_td_alpha = doc_topic_vec_[t] + alpha_ - 1;
				n_sd_alpha = doc_topic_vec_[s] + alpha_ - 1;

				w_t_cnt = get_word_topic(w, t) - 1;
				n_tw_beta = w_t_cnt + beta_;
				n_t_beta_sum = summary_row_[t] + beta_sum_ - 1;

				w_s_cnt = get_word_topic(w, s) - 1;
				n_sw_beta = w_s_cnt + beta_;
				n_s_beta_sum = summary_row_[s] + beta_sum_ - 1;
			}

			proposal_s = (doc_topic_vec_[s] + alpha_);
			proposal_t = (doc_topic_vec_[t] + alpha_);

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
