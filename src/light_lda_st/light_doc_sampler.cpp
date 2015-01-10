// Author: Jinhui Yuan (jiyuan@microsoft.com
// Date:  2014.08.02

#include "light_doc_sampler.hpp"
#include "context.hpp"
#include "utils.hpp"
#include <glog/logging.h>
#include <algorithm>
#include <time.h>
#include <sstream>
#include <string>
#include <fstream>

namespace lda
{
	LightDocSampler::LightDocSampler() : cache_size_(16)
	{
		util::Context& context = util::Context::get_instance();

		K_ = context.get_int32("num_topics");
		V_ = context.get_int32("num_vocabs");
		decay_step_ = context.get_int32("decay_step");

		gs_type_ = context.get_int32("gs_type");
		mh_step_for_gs_ = context.get_int32("mh_step");
		
		beta_ = context.get_double("beta");
		beta_sum_ = beta_ * V_;
		alpha_sum_ = context.get_double("alpha_sum");
		//alpha_sum_ = alpha_ * K_;
		// ORIG: alpha_ = 0.01;
		alpha_ = alpha_sum_ / K_;

		// ORIG: ll_alpha_ = 0.01;
        ll_alpha_ = alpha_;
		ll_alpha_sum_ = ll_alpha_ * K_;

		// Precompute LLH parameters
		//log_doc_normalizer_ = LogGamma(alpha_sum_) - K_ * LogGamma(alpha_);
		//log_doc_normalizer_ = LogGamma(alpha_ * K_) - K_ * LogGamma(alpha_);
		log_doc_normalizer_ = LogGamma(ll_alpha_ * K_) - K_ * LogGamma(ll_alpha_);
		log_topic_normalizer_ = LogGamma(beta_sum_) - V_ * LogGamma(beta_);

		doc_topic_vec_.resize(K_);

		summary_row_.resize(K_,0);

		delta_alpha_sum_ = alpha_sum_ / decay_step_;

#if !defined(USE_HASH)
		word_topic_table_.resize(V_);
		for (auto &row : word_topic_table_)
		{
			row.resize(K_, 0);
		}
#endif
		word_topic_hash_table_.resize(V_);

#ifdef GOOGLE_HASH
		for (int i = 0; i < V_; ++i)
		{
			word_topic_hash_table_[i].set_empty_key(-1);
			word_topic_hash_table_[i].set_deleted_key(-2);
		}
		oplog_.set_empty_key(-1);
		oplog_.set_deleted_key(-2);
#endif
        // ORIG: alias_rng_.Init(K_ * 10);
        alias_rng_.resize(V_);
        for (int i = 0; i < V_; ++i) {
            alias_rng_[i] = std::make_shared<wood::AliasMultinomialRNG>();
            alias_rng_[i]->Init(K_ * 10);
        }
	
		q_w_sum_.resize(V_);
		q_w_proportion_.resize(V_);

		// ORIG: s_w_.resize(V_);
		for (int i = 0; i < V_; i++)
		{
			q_w_proportion_[i].resize(K_);
			// ORIG: s_w_[i].resize(K_);
		}
		// ORIG: s_w_num_.resize(V_);
	}

	LightDocSampler::~LightDocSampler()
	{
	}

	int32_t LightDocSampler::GlobalInit(DocBatch &docs)
	{
		int32_t token_num = 0;

		DocBatch::iterator doc_iter = docs.begin();
		while (doc_iter != docs.end())
		{
			int doc_size = doc_iter->token_num();
			for (int i = 0; i < doc_size; ++i)
			{
				int32_t w = doc_iter->word(i);
				int32_t t = doc_iter->topic(i);

				word_topic_inc(w, t);
				++summary_row_[t];

				++token_num;
			}
			doc_iter++;
		}
		return token_num;
	}

	int32_t LightDocSampler::DocInit(Document *doc)
	{
		//int num_unique_words = doc->num_unique_words();
		int num_words = doc->token_num();

		// Zero out and fill doc_topic_vec_
		std::fill(doc_topic_vec_.begin(), doc_topic_vec_.end(), 0);
		doc->get_doc_topic_vec(doc_topic_vec_);

		doc_size_ = num_words;
		n_td_sum_ = num_words;

		return 0;
	}

	void LightDocSampler::SampleOneDoc(Document *doc)
	{
		switch (gs_type_)
		{
		case 0:
			OldProposalFreshSample(doc);
			break;
		case 1:
			OldProposalOldSample(doc);
			break;
		}
	}

	void LightDocSampler::OldProposalFreshSample(Document *doc)
	{
		DocInit(doc);
		int num_token = doc->token_num();
		std::vector<int32_t> &words = doc->get_words();
		std::vector<int32_t> &topics = doc->get_topics();

		for (int i = 0; i < num_token; ++i)
		{
			++num_sampling_;

			int32_t w = words[i];
			int32_t s = topics[i]; // old topic

			// ORIG:
            /*if (s_w_num_[w] < mh_step_for_gs_ - 1)
			{
				GenerateAliasTableforWord(w);
			}*/
			
			int t = Sample2WordFirst(doc, w, s, s);    // new topic

			if (s != t)
			{
				--summary_row_[s];
				word_topic_dec(w, s);
				--doc_topic_vec_[s];

				++summary_row_[t];
				word_topic_inc(w, t);
				++doc_topic_vec_[t];

				topics[i] = t;
				++num_sampling_changed_;
			}
		}
	}

	void LightDocSampler::OldProposalOldSample(Document *doc)
	{
		DocInit(doc);
		int num_token = doc->token_num();
		num_tokens_ += num_token;
		std::vector<int32_t> &words = doc->get_words();
		std::vector<int32_t> &topics = doc->get_topics();

		for (int i = 0; i < num_token; ++i)
		{
			++num_sampling_;

			int32_t w = words[i];
			int32_t s = topics[i]; // old topic

            // ORIG:
            /*
			if (s_w_num_[w] < mh_step_for_gs_ - 1)
			{
				GenerateAliasTableforWord(w);
			}*/

			int t = SampleWordFirst(doc, w, s);

			if (s != t)
			{
				--summary_row_[s];
				word_topic_dec(w, s);
				--doc_topic_vec_[s];

				++summary_row_[t];
				word_topic_inc(w, t);
				++doc_topic_vec_[t];

				topics[i] = t;
				++num_sampling_changed_;
			}
		}
	}

	double LightDocSampler::ComputeOneDocLLH(Document* doc)
	{
		double doc_ll = 0;

		double one_doc_llh = log_doc_normalizer_;

		// Compute doc-topic vector on the fly.
		std::vector<int32_t> doc_topic_vec(K_);
		int num_words = 0;
		int num_tokens = doc->token_num();

		if (num_tokens == 0)
		{
			return doc_ll;
		}

		num_words = num_tokens;
		doc->get_doc_topic_vec(doc_topic_vec);

		for (int k = 0; k < K_; ++k)
		{
			CHECK_LE(0, doc_topic_vec[k]) << "negative doc_topic_vec[k]";
			// one_doc_llh += LogGamma(doc_topic_vec[k] + alpha_sum_ / K_);
			// one_doc_llh += LogGamma(doc_topic_vec[k] + alpha_);
			one_doc_llh += LogGamma(doc_topic_vec[k] + ll_alpha_);
		}
		// one_doc_llh -= LogGamma(num_words + alpha_sum_);
		one_doc_llh -= LogGamma(num_words + alpha_ * K_);
		CHECK_EQ(one_doc_llh, one_doc_llh) << "one_doc_llh is nan.";

		doc_ll += one_doc_llh;
		return doc_ll;
	}

	double LightDocSampler::ComputeWordLLH()
	{
		// word_llh is P(w|z).
		double word_llh = K_ * log_topic_normalizer_;
		double zero_entry_llh = LogGamma(beta_);

		// Since some vocabs are not present in the corpus, use num_words_seen to
		// count # of words in corpus.
		int num_words_seen = 0;
		for (int w = 0; w < V_; ++w)
		{
			auto &word_topic_row = get_word_row(w);

#ifdef USE_HASH
			if (word_topic_row.size() > 0)
			{
				++num_words_seen;                // increment only when word has non-zero tokens.
				for (auto &topic : word_topic_row)
				{
					// for sparse row
					int count = topic.second;

					CHECK_LE(0, count) << "negative count.";
					word_llh += LogGamma(count + beta_);

					CHECK_EQ(word_llh, word_llh)
						<< "word_llh is nan after LogGamma(count + beta_). count = "
						<< count;
				}
				// The other word-topic counts are 0.
				int num_zeros = K_ - word_topic_row.size();
				word_llh += num_zeros * zero_entry_llh;
			}
#else
			double delta = 0;
			int total_count = 0;
			for (auto &topic : word_topic_row)
			{
				// for sparse row
				int count = topic;
				total_count += count;

				CHECK_LE(0, count) << "negative count.";
				delta += LogGamma(count + beta_);

				CHECK_EQ(delta, delta)
					<< "word_llh is nan after LogGamma(count + beta_). count = "
					<< count;
			}
			if (total_count)
			{
				word_llh += delta;
			}
#endif
		}

		for (int k = 0; k < K_; ++k)
		{
			word_llh -= LogGamma(summary_row_[k] + beta_sum_);
			CHECK_EQ(word_llh, word_llh)
				<< "word_llh is nan after -LogGamma(summary_row[k] + beta_). "
				<< "summary_row[k] = " << summary_row_[k];
		}

		CHECK_EQ(word_llh, word_llh) << "word_llh is nan.";
		return word_llh;
	}

	void LightDocSampler::Dump(const std::string &dump_name)
	{

		std::ofstream wt_stream;
		wt_stream.open(dump_name, std::ios::out);
		CHECK(wt_stream.good()) << "Open word_topic_dump file: " << dump_name;
#ifdef USE_HASH
		for (int w = 0; w < V_; ++w)
		{
			auto &row = word_topic_hash_table_[w];
			if (row.size() > 0)
			{
				wt_stream << w;
				for (auto &col : row)
				{
					wt_stream << " " << col.first << ":" << col.second;
				}
				wt_stream << std::endl;
			}
		}
#else
			
		for (int w = 0; w < V_; ++w)
		{
			int nonzero_num = 0;
			for (int t = 0; t < K_; ++t)
			{
				if (word_topic_table_[w][t] > 0)
				{
					++nonzero_num;
				}
			}
			if (nonzero_num)
			{
				wt_stream << w;
				for (int t = 0; t < K_; ++t)
				{
					if (word_topic_table_[w][t])
					{
						wt_stream << " " << t << ":" << word_topic_table_[w][t];
					}
				}
				wt_stream << std::endl;
			}
		}
#endif

		wt_stream.close();

	}
}
