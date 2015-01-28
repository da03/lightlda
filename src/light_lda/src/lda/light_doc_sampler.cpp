// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date:  2014.08.02

#include "light_doc_sampler.hpp"
#include <time.h>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <glog/logging.h>
#include "lda/context.hpp"
#include "util/utils.hpp"

namespace lda
{
	LightDocSampler::LightDocSampler() : doc_topic_counter_(1024)
	{
		util::Context& context = util::Context::get_instance();

		K_ = context.get_int32("num_topics");
		V_ = context.get_int32("num_vocabs");

		mh_step_for_gs_ = context.get_int32("mh_step");

		beta_ = context.get_double("beta");
		beta_sum_ = beta_ * V_;
		alpha_ = context.get_double("alpha");
		alpha_sum_ = alpha_ * K_;
	}

	LightDocSampler::~LightDocSampler()
	{
	}

	int32_t LightDocSampler::DocInit(LDADocument *doc)
	{
		int num_words = doc->size();

		// Zero out and fill doc_topic_vec_
		doc_topic_counter_.clear();
		doc->GetDocTopicCounter(doc_topic_counter_);
		
		doc_size_ = num_words;
		n_td_sum_ = num_words;
		return 0;
	}

	int32_t LightDocSampler::SampleOneDoc(LDADocument *doc,
		ModelSlice& word_topic_table,
		petuum::ClientSummaryRow& summary_row,
		AliasSlice& alias_table,
		std::vector<std::unique_ptr<petuum::DeltaArray>>& word_topic_delta_vec,
		petuum::SummaryDelta& summary_delta)
	{
		DocInit(doc);
		int num_token = doc->size();
		int32_t num_sampling = 0;
		int32_t num_sampling_changed = 0;
		int32_t slice_id = word_topic_table.SliceId();
		int32_t slice_last_word = word_topic_table.LastWord();
		int32_t& cursor = doc->get_cursor();
		if (slice_id == 0) cursor = 0;
		for (; cursor != doc->size(); ++cursor) {

			int32_t word = doc->Word(cursor);

			if (word > slice_last_word)
				break;

			++num_sampling;
			++num_sampling_;
			int32_t old_topic = doc->Topic(cursor);
			int32_t new_topic = Sample2WordFirst(doc, word, old_topic, old_topic,
				word_topic_table, summary_row, alias_table);
			if (old_topic != new_topic) {
				int32_t shard_id = word % word_topic_delta_vec.size();
				word_topic_delta_vec[shard_id]->Update(word, old_topic, -1);
				doc_topic_counter_.inc(old_topic, -1);
				summary_delta.Update(old_topic, -1);

				word_topic_delta_vec[shard_id]->Update(word, new_topic, 1);
				doc_topic_counter_.inc(new_topic, 1);
				summary_delta.Update(new_topic, 1);

				doc->SetTopic(cursor, new_topic);
				++num_sampling_changed_;
				++num_sampling_changed;
			}
		}
		return num_sampling;
	}

	void LightDocSampler::InferOneDoc(LDADocument* doc, ModelSlice& word_topic_table,
		petuum::ClientSummaryRow& summary_row, AliasSlice& alias_table) 
	{
		DocInit(doc);
		int num_token = doc->size();

		int32_t slice_id = word_topic_table.SliceId();
		int32_t slice_last_word = word_topic_table.LastWord();

		int32_t& cursor = doc->get_cursor();
		if (slice_id == 0) cursor = 0;
		for (; cursor != doc->size(); ++cursor) {
			int32_t word = doc->Word(cursor);
			if (word > slice_last_word)
				break;

			int32_t old_topic = doc->Topic(cursor);
			int32_t new_topic = InferWordFirst(doc, word, old_topic, old_topic,
				word_topic_table, summary_row, alias_table);

			if (old_topic != new_topic) {
				doc_topic_counter_.inc(old_topic, -1);
				doc_topic_counter_.inc(new_topic, 1);
				doc->SetTopic(cursor, new_topic);
			}
		}
	}
}