// Author: Gao Fei (v-feigao@microsoft.com)
// Date: 2014.10.15
#pragma once

#include <cstdint>
#include <vector>
#include "memory/summary_row.hpp"
#include "util/hybrid_map.h"
#include "util/record_buff.hpp"


namespace petuum {
	class ClientSendOpLogIterationMsg;
	class DeltaArray;
}

namespace lda {
	class LocalVocab;
	class DeltaSlice {
	public:
		DeltaSlice();
		~DeltaSlice();

		// Must Init before called other method
		void Init(LocalVocab* local_vocab, int32_t slice_id);

		// aggregate local delta
		void MergeFrom(const petuum::DeltaArray& delta_array);

		typedef void(*ClientSendDeltaMsgFunc)(int32_t recv_id,
			petuum::ClientSendOpLogIterationMsg* msg, bool is_last, bool is_iteration_clock);

		// serialize model
		int64_t ClientCreateSendTableDeltaMsg(
			ClientSendDeltaMsgFunc SendMsg,
			bool is_iteration_clock);

		void GenerateRow();
	private:
		// hybrid_map GetRow(int32_t word);

		void Update(int32_t word, int32_t topic, int32_t update);

		// Serialize
		bool AppendTableToBuffs(
			std::unordered_map<int32_t, petuum::RecordBuff>* buffs,
			int32_t& failed_server_id, bool resume);
		bool AppendRowToBuffs(
			std::unordered_map<int32_t, petuum::RecordBuff>* buffs,
			const void* row_data, size_t row_size, int32_t row_id, int32_t& failed_server_id);
	private:
		int32_t* memory_block_;
		int64_t memory_block_size_;

		std::vector<lda::hybrid_map> table_;

		LocalVocab* local_vocab_;
		int32_t slice_id_;

		int32_t num_delta_threads_;

		int64_t nonzero_entries_;

		// int32_t* rehashing_buf_;
		std::vector<int32_t*> rehashing_buf_;
		// Serialize
		static const size_t kSendDeltaMsgSizeInit = 16 * 1024 * 1024; // 16MB
		static const size_t kTmpRowBuffSizeInit = 64 * 1024; // 64KB
		int32_t index_iter_; // use index_iter_ to traverse the vector
		size_t send_msg_data_size_;
		size_t tmp_row_buff_size_;
		size_t curr_row_size_;
		uint8_t* tmp_row_buff_;
	};
}