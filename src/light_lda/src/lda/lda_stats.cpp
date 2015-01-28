// Author: Gao Fei (v-feigao@microsoft.com)
// Date: 2014.10.20

#include "lda/lda_stats.hpp"
#include <algorithm>
#include <iostream>
#include <vector>
#include <sstream>
#include <string>
#include <glog/logging.h>
#include "lda/context.hpp"
#include "util/utils.hpp"
#include "util/light_hash_map.h"

namespace lda {

	LDAStats::LDAStats() {
		// Topic model parameters.
		util::Context& context = util::Context::get_instance();
		K_ = context.get_int32("num_topics");
		V_ = context.get_int32("num_vocabs");

		CHECK_NE(-1, V_);
		beta_ = context.get_double("beta");
		beta_sum_ = beta_ * V_;
		alpha_ = context.get_double("alpha");
		alpha_sum_ = K_ * alpha_;
		num_threads_ = context.get_int32("num_worker_threads");
		// Precompute LLH parameters
		real_t ll_alpha = 0.01;
		log_doc_normalizer_ = LogGamma(K_ * ll_alpha) - K_ * LogGamma(ll_alpha);
		// log_doc_normalizer_ = LogGamma(alpha_sum_) - K_ * LogGamma(alpha_);
		log_topic_normalizer_ = LogGamma(beta_sum_) - V_ * LogGamma(beta_);

	}

	double LDAStats::ComputeOneDocLLH(LDADocument* doc) {
		double one_doc_llh = log_doc_normalizer_;

		wood::light_hash_map doc_topic_counter(1024);
		doc->GetDocTopicCounter(doc_topic_counter);
		int num_words = doc->size();
		if (num_words == 0) 
			return 0.0;
		int32_t capacity = doc_topic_counter.capacity();
		int32_t *key = doc_topic_counter.key();
		int32_t *value = doc_topic_counter.value();
		int32_t nonzero_num = 0;

		real_t ll_alpha = 0.01;

		for (int i = 0; i < capacity; ++i) {
			if (key[i] > 0) {
				one_doc_llh += LogGamma(value[i] + ll_alpha);
				++nonzero_num;
			}
		}

		one_doc_llh += (K_ - nonzero_num) * LogGamma(ll_alpha);
		one_doc_llh -= LogGamma(num_words + ll_alpha * K_);

		CHECK_EQ(one_doc_llh, one_doc_llh) << "one_doc_llh is nan.";
		return one_doc_llh;
	}

	double LDAStats::ComputeOneSliceWordLLH(
		ModelSlice& word_topic_table,
		int32_t thread_id) {
		double word_llh = 0;
		double zero_entry_llh = LogGamma(beta_);

		for (int32_t word_index = Begin(thread_id);
			word_index != End(thread_id);
			++word_index) {
			int32_t num_entries = 0;
			hybrid_map row = word_topic_table.GetRowByIndex(word_index);

			int32_t total_count = 0;
			double delta = 0;
			if (row.is_dense()) {
				int32_t* memory = row.memory();
				int32_t capacity = row.capacity();
				int32_t count;
				for (int i = 0; i < capacity; ++i) {
					count = memory[i];
					CHECK_LE(0, count) << "negative count . " << count;
					total_count += count;
					delta += LogGamma(count + beta_);
				}
			}
			else {
				int32_t* key = row.key();
				int32_t* value = row.value();
				int32_t capacity = row.capacity();
				int32_t count = 0;
				int32_t nonzero_num = 0;
				for (int i = 0; i < capacity; ++i) {
					if (key[i] > 0) {
						count = value[i];
						CHECK_LE(0, count) << "negative count . " << count;
						total_count += count;
						delta += LogGamma(count + beta_);
						++nonzero_num;
					}
 				}
				if (nonzero_num != 0) delta += (K_ - nonzero_num) * zero_entry_llh;
			}

			if (total_count) {
				word_llh += delta;
			}

			// int32_t word = word_topic_table.SliceId() * slice_size_ + word_index;
			// int32_t word = local_vocab_->IndexToWord()
			//for (int k = 0; k < K_; ++k) {
			//	int32_t count = word_topic_table.GetIndexTopicCount(word_index, k); //GetWordTopicCount(word, k);
			//	CHECK_GE(count, 0) << "Negative count";
			//	if (count > 0) {
			//		word_llh += LogGamma(count + beta_);
			//		++num_entries;
			//	}
			//}
			//if (num_entries != 0) {
			//	word_llh += (K_ - num_entries) * zero_entry_llh;
			//}
		}
		CHECK_EQ(word_llh, word_llh) << "word_llh is nan.";
		return word_llh;
	}

	double LDAStats::NormalizeWordLLH(petuum::ClientSummaryRow& summary_row) {
		double word_llh = K_ * log_topic_normalizer_;
		for (int k = 0; k < K_; ++k) {
			int64_t count = summary_row.GetSummaryCount(k);
			CHECK_GE(count, 0);
			word_llh -= LogGamma(count + beta_sum_);
			CHECK_EQ(word_llh, word_llh) << "word_llh is nan after -LogGamma";
		}
		return word_llh;
	}

	//double LDAStats::ComputeWordLLH(
	//	WordTopicTable& word_topic_table,
	//	SummaryTable& summary_table) {
	//	V_ = util::Context::get_instance().get_int32("num_vocabs");
	//	LOG(INFO) << "Compute Word LLH";
	//	// word_llh is P(w|z).
	//	double word_llh = K_ * log_topic_normalizer_;
	//	double zero_entry_llh = LogGamma(beta_);

	//	//for (int w = 0; w < V_; ++w) {
	//	//for (auto word_topic : word_topic_table) {
	//	//	int32_t word_id = word_topic.first;
	//	//	const WordTopicRow& word_topic_row = *(word_topic.second);

	//	//	if (word_topic_row.num_entries() > 0) {
	//	//		//++num_words_seen;  // increment only when word has non-zero tokens.
	//	//		for (WordTopicRow::const_iterator wt_it =
	//	//			word_topic_row.cbegin(); !wt_it.is_end(); ++wt_it) {
	//	//			int count = wt_it->second;
	//	//			CHECK_LE(0, count) << "negative count.";
	//	//			word_llh += LogGamma(count + beta_);
	//	//			CHECK_EQ(word_llh, word_llh)
	//	//				<< "word_llh is nan after LogGamma(count + beta_). count = "
	//	//				<< count;
	//	//		}
	//	//		// The other word-topic counts are 0.
	//	//		int num_zeros = K_ - word_topic_row.num_entries();
	//	//		word_llh += num_zeros * zero_entry_llh;
	//	//	}
	//	//}
	//	for (int w = 0; w < V_; ++w) {
	//		const WordTopicRow& word_topic_row = *(word_topic_table[w]);
	//		int num_entries = 0;
	//		for (int k = 0; k < K_; ++k) {
	//			int count = word_topic_row[k];
	//			CHECK_LE(0, count) << "negative count";
	//			if (count > 0) {
	//				word_llh += LogGamma(count + beta_);
	//				num_entries++;
	//			}
	//		}
	//		if (num_entries != 0) {
	//			word_llh += (K_ - num_entries) * zero_entry_llh;
	//		}
	//	}
	//	const auto& summary_row = *summary_table[0];
	//	for (int k = 0; k < K_; ++k) {
	//		word_llh -= LogGamma(summary_row[k] + beta_sum_);
	//		CHECK_EQ(word_llh, word_llh)
	//			<< "word_llh is nan after -LogGamma(summary_row[k] + beta_). "
	//			<< "summary_row[k] = " << summary_row[k];
	//	}

	//	CHECK_EQ(word_llh, word_llh) << "word_llh is nan.";
	//	return word_llh;
	//}

}  // namespace lda
