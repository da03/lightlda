// Author: Gao Fei(v-feigao@microsoft.com)
// Data: 2014-10-11

#include "memory/local_vocab.h"
#include <fstream>
#include <memory>

namespace lda {
	LocalVocab::LocalVocab() : has_read_(false) {
		util::Context& context = util::Context::get_instance();
		V_ = context.get_int32("num_vocabs");
		num_threads_ = context.get_int32("num_worker_threads");
		vocab_map_.resize(V_, -1);
	}

	LocalVocab::~LocalVocab() {
		if (has_read_) {
			delete[] local_tf_;
			delete[] tf_;
			delete[] vocab_;
		}
	}

	void LocalVocab::Read(const std::string& file_name) {
		std::ifstream vocab_file(file_name, std::ios::in | std::ios::binary);
		CHECK(vocab_file.good()) << "Fails to open file: " << file_name;

		vocab_file.read(reinterpret_cast<char*>(&vocab_size_), sizeof(int32_t));
		
		vocab_ = new int32_t[vocab_size_];
		tf_ = new int32_t[vocab_size_];
		local_tf_ = new int32_t[vocab_size_];
		vocab_file.read(reinterpret_cast<char*>(vocab_), sizeof(int32_t)* vocab_size_);
		vocab_file.read(reinterpret_cast<char*>(tf_), sizeof(int32_t)* vocab_size_);
		vocab_file.read(reinterpret_cast<char*>(local_tf_), sizeof(int32_t)* vocab_size_);
		vocab_file.close();

		for (int i = 0; i < vocab_size_; ++i) {
			vocab_map_[vocab_[i]] = i;
		}

		// Compute Meta Information;
		GenerateMetaForModelSlice();
		has_read_ = true;
	}

	void LocalVocab::SerializeAs(void* bytes, int32_t size, int32_t slice_id) const {
		int32_t* data_ptr = reinterpret_cast<int32_t*>(bytes);
		int32_t slice_size = slice_index_[slice_id + 1] - slice_index_[slice_id];
		*data_ptr++ = slice_size;
		memcpy(data_ptr, vocab_ + slice_index_[slice_id], sizeof(int32_t)* slice_size);
	}

	void LocalVocab::GenerateMetaForModelSlice() {
		
		util::Context& context = util::Context::get_instance();
		
		int32_t num_topics = context.get_int32("num_topics");
		int32_t load_factor = context.get_int32("load_factor");

		int64_t model_max_capacity = context.get_int64("model_max_capacity");
		int64_t alias_max_capacity = context.get_int64("alias_max_capacity");
		int64_t delta_max_capacity = context.get_int64("delta_max_capacity");

		int32_t model_hot_thresh = num_topics / (2 * load_factor); 
		int32_t alias_hot_thresh = (num_topics * 2) / 3; 
		int32_t delta_hot_thresh = num_topics / (4 * load_factor); 

		int64_t model_offset = 0;
		int64_t alias_offset = 0;
		int64_t delta_offset = 0;

		std::unique_ptr<SliceMeta> slice_meta(new SliceMeta);
		slice_index_.push_back(0);
		for (int i = 0; i < vocab_size_; ++i) {
			WordEntry word_entry;
			int32_t word_id = vocab_[i];
			int32_t tf = tf_[i];
			int32_t local_tf = local_tf_[i];

			int32_t capacity, table_size;
			if (tf >= model_hot_thresh) {
				word_entry.is_model_dense_ = 1;
				capacity = num_topics;
				table_size = capacity;
			}
			else {
				word_entry.is_model_dense_ = 0;
				int32_t capacity_lower_bound = load_factor * tf;
				capacity = upper_bound(capacity_lower_bound);
				table_size = capacity * 2;
			}
			word_entry.offset_ = model_offset;
			word_entry.end_offset_ = model_offset + table_size;
			word_entry.capacity_ = capacity;
			model_offset += table_size;

			int32_t alias_capacity, alias_buf_size;
			if (tf >= alias_hot_thresh) {
				word_entry.is_alias_dense_ = 1;
				alias_buf_size = 2 * num_topics;
				alias_capacity = num_topics;
			}
			else {
				word_entry.is_alias_dense_ = 0;
				alias_buf_size = 3 * tf;
				alias_capacity = tf;
			}
			word_entry.alias_capacity_ = alias_capacity;
			word_entry.alias_offset_ = alias_offset;
			word_entry.alias_end_offset_ = alias_offset + alias_buf_size;
			alias_offset += alias_buf_size;

			int32_t delta_capacity, delta_buf_size;
			if (local_tf >= delta_hot_thresh) {
				word_entry.is_delta_dense_ = 1;
				delta_buf_size = num_topics;
				delta_capacity = num_topics;
			}
			else {
				word_entry.is_delta_dense_ = 0;
				int32_t capacity_lower_bound = load_factor * 2 * local_tf;
				delta_capacity = upper_bound(capacity_lower_bound);
				delta_buf_size = 2 * delta_capacity;
			}
			word_entry.delta_capacity_ = delta_capacity;
			word_entry.delta_offset_ = delta_offset;
			word_entry.delta_end_offset_ = delta_offset + delta_buf_size;
			delta_offset += delta_buf_size;

			if (model_offset > model_max_capacity || 
				alias_offset > alias_max_capacity ||
				delta_offset > delta_max_capacity) {
				// add into MetaVector.
				slice_meta_.push_back(std::move(*slice_meta));
				slice_index_.push_back(i);
				slice_meta.reset(new SliceMeta);

				model_offset = 0; alias_offset = 0; delta_offset = 0;

				word_entry.offset_ = model_offset;
				word_entry.end_offset_ = model_offset + table_size;
				model_offset += table_size;

				word_entry.alias_offset_ = alias_offset;
				word_entry.alias_end_offset_ = alias_offset + alias_buf_size;
				alias_offset += alias_buf_size;

				word_entry.delta_offset_ = delta_offset;
				word_entry.delta_end_offset_ = delta_offset + delta_buf_size;
				delta_offset += delta_buf_size;
			}
			
			slice_meta->push_back(word_entry);
		}

		if (!slice_meta->empty()) {
			slice_meta_.push_back(std::move(*slice_meta));
			slice_index_.push_back(vocab_size_);
		}

		num_of_slice_ = slice_meta_.size();
		LOG(INFO) << "Finish to generate meta information. Num of slice = " << num_of_slice_;
	}
}