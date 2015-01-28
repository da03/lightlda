// Author: Gao Fei (v-feigao@microsoft.com)
// Date: 2014.10.15

#include "memory/model_slice.h"
#include "system/ps_msgs.hpp"
#include "util/serialized_row_reader.hpp"

namespace lda {
	ModelSlice::ModelSlice() {
		util::Context& context = util::Context::get_instance();
		memory_block_size_ = context.get_int64("model_max_capacity");
		try {
			memory_block_ = new int32_t[memory_block_size_];
		}
		catch (std::bad_alloc& ba) {
            LOG(ERROR) << memory_block_size_;
			LOG(FATAL) << "Bad Alloc caught: " << ba.what();
		}

		int32_t V = context.get_int32("num_vocabs");
		table_.resize(V);
		int32_t K = context.get_int32("num_topics");
		rehashing_buf_ = new int32_t[K];
	}

	ModelSlice::~ModelSlice() {
		delete[] rehashing_buf_;
		delete[] memory_block_;
	}

	void ModelSlice::Init(LocalVocab* local_vocab, int32_t slice_id) {
		CHECK(local_vocab != nullptr);
		local_vocab_ = local_vocab;
		slice_id_ = slice_id;

		memset(memory_block_, 0, memory_block_size_ * sizeof(int32_t));
		GenerateRow();
	}

	int32_t ModelSlice::GetWordTopicCount(int32_t word, int32_t topic) {
		int32_t index = local_vocab_->WordToIndex(slice_id_, word);
		return table_[index][topic];
	}

	int32_t ModelSlice::GetIndexTopicCount(int32_t index, int32_t topic) {
		return table_[index][topic];
	}

	int64_t ModelSlice::ApplyServerModelSliceRequestReply(
		petuum::ServerPushOpLogIterationMsg& msg) {
		int64_t num_entries = 0; // for debug

		int32_t table_id;
		petuum::SerializedRowReader row_reader(msg.get_data(), msg.get_avai_size());
		bool to_read = row_reader.Restart();
		int32_t row_id;
		size_t row_size;
		if (to_read) {
			const void* data = row_reader.Next(&table_id, &row_id, &row_size);
			while (data != nullptr) {
				num_entries += ApplyRowOpLog(row_id, data, row_size);
				data = row_reader.Next(&table_id, &row_id, &row_size);
			}
		}
		return num_entries;
	}

	int32_t ModelSlice::ApplyRowOpLog(int32_t row_id,
		const void* data, size_t row_size) {
		CHECK_GT(row_size, 0);
		int32_t index = local_vocab_->WordToIndex(slice_id_, row_id);
		table_[index].ApplySparseBatchInc(data, row_size);
		// LOG(INFO) << "get word_id = " << row_id;
		int32_t num_entries = row_size / (sizeof(int32_t)+sizeof(int32_t));
		UpdateMetaForAliasTable(row_id, num_entries);
		return num_entries;
	}
	
	void ModelSlice::GenerateRow() {
		int32_t size = local_vocab_->SliceSize(slice_id_);
		SliceMeta& dict = local_vocab_->Meta(slice_id_);
		
		for (int32_t index = 0; index < size; ++index) {
			table_[index] = lda::hybrid_map(
				memory_block_ + dict[index].offset_,
				dict[index].is_model_dense_,
				dict[index].capacity_, 
				rehashing_buf_);
		}
	}
}
