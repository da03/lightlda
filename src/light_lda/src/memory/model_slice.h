// Author: Gao Fei (v-feigao@microsoft.com)
// Date: 2014.10.15

#pragma once

#include <cstdint>
#include <vector>
#include "memory/local_vocab.h"
#include "util/record_buff.hpp"
#include "util/hybrid_map.h"

namespace petuum {
	class ServerPushOpLogIterationMsg;
}

namespace lda {
	class ModelSlice {
	public:
		ModelSlice();
		~ModelSlice();

		// Must Init before called other method
		void Init(LocalVocab* local_vocab, int32_t slice_id);

		// access Model
		int32_t GetWordTopicCount(int32_t word, int32_t topic);

		int32_t GetIndexTopicCount(int32_t index, int32_t topic);

		// write Model
		int64_t ApplyServerModelSliceRequestReply(
			petuum::ServerPushOpLogIterationMsg& msg);

		int32_t SliceId() const;
		LocalVocab* GetLocalVocab() const;
		int32_t LastWord() const;

		void GenerateRow();

		lda::hybrid_map& GetRow(int32_t word);
		lda::hybrid_map& GetRowByIndex(int32_t index) {
			return table_[index];
		}
	
	private:
		void Update(int32_t word, int32_t topic, int32_t update);

		int32_t ApplyRowOpLog(int32_t row_id, const void* data, size_t row_size);

		void UpdateMetaForAliasTable(int32_t word, int32_t non_zero_entries);

	private:
		int32_t* memory_block_;
		int64_t memory_block_size_;

		std::vector<lda::hybrid_map> table_;

		int32_t* rehashing_buf_;

		LocalVocab* local_vocab_;
		int32_t slice_id_;
	};

	inline int32_t ModelSlice::SliceId() const {
		return slice_id_;
	}

	inline LocalVocab* ModelSlice::GetLocalVocab() const {
		return local_vocab_;
	}

	inline int32_t ModelSlice::LastWord() const{
		return local_vocab_->LastWord(slice_id_);
	}

	inline lda::hybrid_map& ModelSlice::GetRow(int32_t word) {
		int32_t index = local_vocab_->WordToIndex(slice_id_, word);
		CHECK(index >= 0 && index < local_vocab_->SliceSize(slice_id_));
		return table_[index];
	}

	inline void ModelSlice::UpdateMetaForAliasTable(
		int32_t word, int32_t non_zero_entries) {
		int32_t index = local_vocab_->WordToIndex(slice_id_, word);
		SliceMeta&dict = local_vocab_->Meta(slice_id_);
		if (dict[index].is_alias_dense_ == 0) {
			dict[index].alias_capacity_ = non_zero_entries;
		}
	}
}