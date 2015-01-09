// Author: Jinhui Yuan (jiyuan@microsoft.com
// Date:  2014.08.02

#include "light_doc_sampler.hpp"
#include "context.hpp"
#include <glog/logging.h>
#include <algorithm>
#include <time.h>
#include <sstream>
#include <string>
#include <fstream>

namespace lda
{
	LightDocSampler::LightDocSampler(
		std::vector<lda::hybrid_map> &word_topic_table,
		std::vector<int64_t> &summary_row,
		std::vector<lda::hybrid_alias_map> &alias_kv,
		int32_t beta_height,
		float beta_mass,
		std::vector<wood::alias_k_v> beta_k_v)
		: doc_topic_counter_(1024),
		word_topic_table_(word_topic_table), summary_row_(summary_row),
		alias_k_v_(alias_kv),
		beta_height_(beta_height),
		beta_mass_(beta_mass),
		beta_k_v_(beta_k_v)
	{
		util::Context& context = util::Context::get_instance();

		K_ = context.get_int32("num_topics");
		V_ = context.get_int32("num_vocabs");

		num_threads_ = context.get_int32("num_threads");

		mh_step_for_gs_ = context.get_int32("mh_step");

		beta_ = context.get_double("beta");
		beta_sum_ = beta_ * V_;
		alpha_sum_ = context.get_double("alpha_sum");
		alpha_ = alpha_sum_ / K_;

		ll_alpha_ = 0.01;
		ll_alpha_sum_ = ll_alpha_ * K_;

		// Precompute LLH parameters
		log_doc_normalizer_ = LogGamma(ll_alpha_ * K_) - K_ * LogGamma(ll_alpha_);
		log_topic_normalizer_ = LogGamma(beta_sum_) - V_ * LogGamma(beta_);

		alias_rng_.Init(K_);

		q_w_proportion_.resize(K_);
		delta_summary_row_.resize(K_);
		word_topic_delta_.resize(num_threads_);

		rehashing_buf_ = new int32_t[K_ * 2];
	}
	

	LightDocSampler::~LightDocSampler()
	{
		delete[] rehashing_buf_;
	}

	// Initialize word_topic_table and doc_topic_counter for each doc
	int32_t LightDocSampler::GlobalInit(LDADocument *doc)
	{
		int32_t token_num = 0;
		int32_t doc_size = doc->size();
		for (int i = 0; i < doc_size; ++i)
		{
			int32_t w = doc->Word(i);
			int32_t t = doc->Topic(i);

			/*
			word_topic_inc(w, t);
			++summary_row_[t];
			*/

			word_topic_delta wtd;
			int32_t shard_id = w % num_threads_;
			wtd.word = w;
			wtd.topic = t;
			wtd.delta = 1;
			word_topic_delta_[shard_id].push_back(wtd);

			++delta_summary_row_[t];			

			++token_num;
		}
		return token_num;
	}

	int32_t LightDocSampler::DocInit(LDADocument *doc)
	{
		int num_words = doc->size();

		// compute the doc_topic_counter on the fly
		doc_topic_counter_.clear();
		doc->GetDocTopicCounter(doc_topic_counter_);
	
		doc_size_ = num_words;
		n_td_sum_ = num_words;

		return 0;
	}

	int32_t LightDocSampler::SampleOneDoc(LDADocument *doc, int32_t slice_upper)
	{
		return  OldProposalFreshSample(doc, slice_upper);
	}

	int32_t LightDocSampler::OldProposalFreshSample(LDADocument *doc, int32_t slice_upper)
	{

		DocInit(doc);
		int num_token = doc->size();
		int32_t &cursor = doc->get_cursor();

		int32_t token_sweeped = 0;

		while (cursor < num_token)
		{
			++num_sampling_;
			++token_sweeped;

			int32_t w = doc->Word(cursor);
			int32_t s = doc->Topic(cursor);  // old topic
			
			if (w >= slice_upper)
			{
				break;
			}

			int t = Sample2WordFirst(doc, w, s, s);    // new topic

			if (s != t)
			{
				/*
				--summary_row_[s];
				word_topic_dec(w, s);
				++summary_row_[t];
				word_topic_inc(w, t);
				*/

				
				word_topic_delta wtd;
				int32_t shard_id = w % num_threads_;
				wtd.word = w;
				wtd.topic = s;
				wtd.delta = -1;
				word_topic_delta_[shard_id].push_back(wtd);

				wtd.topic = t;
				wtd.delta = +1;
				word_topic_delta_[shard_id].push_back(wtd);

				--delta_summary_row_[s];
				++delta_summary_row_[t];
				

				doc->SetTopic(cursor,t);
				doc_topic_counter_.inc(s, -1);
				doc_topic_counter_.inc(t, 1);

				++num_sampling_changed_;
			}
			cursor++;
		}
		if (cursor == num_token)
		{
			cursor = 0;
		}
		return token_sweeped;
	}
	
	double LightDocSampler::ComputeOneDocLLH(LDADocument* doc)
	{
		double doc_ll = 0;
		double one_doc_llh = log_doc_normalizer_;

		// Compute doc-topic vector on the fly.
		int num_tokens = doc->size();

		if (num_tokens == 0)
		{
			return doc_ll;
		}

		doc_topic_counter_.clear();
		doc->GetDocTopicCounter(doc_topic_counter_);

		int32_t capacity = doc_topic_counter_.capacity();
		int32_t *key = doc_topic_counter_.key();
		int32_t *value = doc_topic_counter_.value();
		int32_t nonzero_num = 0;

		for (int i = 0; i < capacity; ++i)
		{
			if (key[i] > 0)
			{
				one_doc_llh += LogGamma(value[i] + ll_alpha_);
				++nonzero_num;
			}
		}
		one_doc_llh += (K_ - nonzero_num) * LogGamma(ll_alpha_);
		one_doc_llh -= LogGamma(num_tokens + ll_alpha_ * K_);

		CHECK_EQ(one_doc_llh, one_doc_llh) << "one_doc_llh is nan.";

		doc_ll += one_doc_llh;
		return doc_ll;
	}

	double LightDocSampler::ComputeWordLLH(int32_t lower, int32_t upper)
	{
		// word_llh is P(w|z).
		double word_llh = 0;
		double zero_entry_llh = LogGamma(beta_);

		// Since some vocabs are not present in the corpus, use num_words_seen to
		// count # of words in corpus.
		int num_words_seen = 0;
		for (int w = lower; w < upper; ++w)
		{
			auto word_topic_row = get_word_row(w);
			int32_t total_count = 0;
			double delta = 0;
			if (word_topic_row.is_dense())
			{
				int32_t* memory = word_topic_row.memory();
				int32_t capacity = word_topic_row.capacity();
				int32_t count;
				for (int i = 0; i < capacity; ++i)
				{
					count = memory[i];
					CHECK_LE(0, count) << "negative count. " << count;
					total_count += count;
					delta += LogGamma(count + beta_);
				}
			}
			else
			{
				int32_t* key = word_topic_row.key();
				int32_t* value = word_topic_row.value();
				int32_t capacity = word_topic_row.capacity();
				int32_t count;
				int32_t nonzero_num = 0;
				for (int i = 0; i < capacity; ++i)
				{
					if (key[i] > 0)
					{
						count = value[i];
						CHECK_LE(0, count) << "negative count. " << count;
						total_count += count;
						delta += LogGamma(count + beta_);
						++nonzero_num;
					}
				}
				delta += (K_ - nonzero_num) * zero_entry_llh;
			}

			if (total_count)
			{
				word_llh += delta;
			}
		}

		CHECK_EQ(word_llh, word_llh) << "word_llh is nan.";
		return word_llh;
	}

	void LightDocSampler::Dump(const std::string &dump_name, int32_t lower, int32_t upper)
	{

		std::ofstream wt_stream;
		wt_stream.open(dump_name, std::ios::out);
		CHECK(wt_stream.good()) << "Open word_topic_dump file: " << dump_name;
	
		for (int w = lower; w < upper; ++w)
		{
			int nonzero_num = word_topic_table_[w].nonzero_num();
			if (nonzero_num)
			{
				wt_stream << w;
				for (int t = 0; t < K_; ++t)
				{
					// if (word_topic_table_[w * K_ + t])
					// if (word_topic_table_[(int64_t)w * K_ + t])
					if (word_topic_table_[w][t] > 0)
					{
						wt_stream << " " << t << ":" << word_topic_table_[w][t];
					}
				}
				wt_stream << std::endl;
			}
		}
		wt_stream.close();
	}
}