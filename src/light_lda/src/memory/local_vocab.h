// Author: Gao Fei(v-feigao@microsoft.com)
// Data: 2014-10-11

#pragma once

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <vector>
#include "lda/context.hpp"

namespace lda {

	struct WordEntry {
		int32_t is_model_dense_;
		int64_t offset_;
		int64_t end_offset_;
		int32_t capacity_; // global_tf
		
		int32_t is_alias_dense_;
		int64_t alias_offset_;
		int64_t alias_end_offset_;
		int32_t alias_capacity_; // num_non_zero

		int32_t is_delta_dense_;
		int64_t delta_offset_;
		int64_t delta_end_offset_;
		int32_t delta_capacity_; // 2 * local_tf
	};

	// Meta information for one slice
	typedef std::vector<WordEntry> SliceMeta;
	
	
	class LocalVocab {
	public:
		LocalVocab();
		~LocalVocab();

		// All method should be called after Read
		void Read(const std::string& file_name);

		int32_t NumOfSlice() const;

		// interface for ModelIO.
		// Msg data format: num_words, word1, word2, ... wordn
		int32_t MsgSize(int32_t slice_id) const;
		void SerializeAs(void* bytes, int32_t size, int32_t slice_id) const;

		// LastWord of current slice, used by SampleOneDoc, loop break when cursor.Topic > LastWord
		int32_t LastWord(int32_t slice_id) const;
		int32_t FirstWord(int32_t slice_id) const;
		
		int32_t SliceSize(int32_t slice_id) const;
		SliceMeta& Meta(int32_t slice_id);
		// get WordID based on the index of Model/Delta/Alias Table.
		int32_t IndexToWord(int32_t slice_id, int32_t index) const;
		// get index of Model/Delta/Alias Table based on word_id and slice_id
		int32_t WordToIndex(int32_t slice_id, int32_t word) const;

		// function for logging
		int64_t GlobalTFSum(int32_t slice_id) {
			return std::accumulate(tf_ + slice_index_[slice_id], tf_ + slice_index_[slice_id+1], int64_t(0));
		}
		int64_t LocalTFSum(int32_t slice_id) {
			return std::accumulate(local_tf_ + slice_index_[slice_id], local_tf_ + slice_index_[slice_id+1], int64_t(0));
		}
	private:
		void GenerateMetaForModelSlice();
		int32_t upper_bound(int32_t x);

	private:
		bool has_read_;
		int32_t* vocab_;
		int32_t* tf_;
		int32_t* local_tf_;
		int32_t vocab_size_;
		std::vector<int32_t> vocab_map_;

		std::vector<int32_t> slice_index_;
		std::vector<SliceMeta> slice_meta_;
		int32_t num_of_slice_;

		int32_t V_;
		int32_t num_threads_;
	};

	typedef std::vector<LocalVocab> Vocabs;

	inline int32_t LocalVocab::NumOfSlice() const {
		CHECK(has_read_) << "Vocabs have not been load";
		return num_of_slice_;
	}

	inline int32_t LocalVocab::MsgSize(int32_t slice_id) const {
		return (1 + SliceSize(slice_id)) * sizeof(int32_t);
	}

	inline SliceMeta& LocalVocab::Meta(int32_t slice_id) {
		return slice_meta_[slice_id];
	}

	inline int32_t LocalVocab::LastWord(int32_t slice_id) const {
		int32_t index = slice_index_[slice_id + 1] - 1;
		CHECK(index >= 0 && index < vocab_size_);
		return vocab_[index];
	}
	inline int32_t LocalVocab::FirstWord(int32_t slice_id) const {
		int32_t index = slice_index_[slice_id];
		return vocab_[index];
	}

	inline int32_t LocalVocab::SliceSize(int32_t slice_id) const {
		CHECK(slice_id < num_of_slice_);
		return slice_index_[slice_id + 1] - slice_index_[slice_id];
	}

	inline int32_t LocalVocab::IndexToWord(int32_t slice_id, int32_t index) const {
		int32_t index_of_vocab = slice_index_[slice_id] + index;
		CHECK(index_of_vocab < vocab_size_) 
			<< "slice_id = " << slice_id << ". index = " << index;
		return vocab_[index_of_vocab];
	}

	inline int32_t LocalVocab::WordToIndex(int32_t slice_id, int32_t word) const {
		if (vocab_map_[word] == -1) return -1;
		CHECK_NE(vocab_map_[word], -1);
		return vocab_map_[word] - slice_index_[slice_id];
	}

	inline int32_t LocalVocab::upper_bound(int32_t x) {
		int32_t shift = 0;
		int32_t y = 1;
		x--;
		while (x) {
			x = x >> 1; y = y << 1; ++shift;
		}
		return y;
	}
		
}