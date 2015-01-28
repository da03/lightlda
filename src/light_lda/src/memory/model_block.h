// Author: Gao Fei (v-feigao@microsoft.com)
// Data: 2014.10.08

#pragma once

#include "util/hybrid_map.h"
#include "system/system_context.hpp"
#include "util/striped_lock.hpp"

namespace petuum {
	using lda::hybrid_map;

	class ClientModelSliceRequestMsg;
	class ClientSendOpLogIterationMsg;
	class ServerPushOpLogIterationMsg;
	class RecordBuff;

	struct WordEntry
	{
		int32_t word_id_;
		int64_t offset_;
		int64_t end_offset_;
		int32_t capacity_;
		int32_t is_dense_;
	};

	class LDAModelBlock
	{
		enum {
			kLockPoolSize = 41
		};
	public:
		explicit LDAModelBlock(int32_t client_id);
		~LDAModelBlock();

		inline lda::hybrid_map& get_row(int word_id)
		{
			int32_t index = RowIDToIndex(word_id);
			CHECK(index >= 0 && index < num_vocabs_) << "Row id = " << word_id << " index = " << index;
			return table_[index];
		}
		void Read(const std::string &meta_name);

		void Dump(const std::string &file_name);

		void ApplyClientSendOpLogIterationMsg(ClientSendOpLogIterationMsg& msg);
		void ApplyRowOpLog(int32_t table_id, int32_t row_id,
			const void *data, size_t row_size);
		typedef void(*ServerPushModelSliceMsgFunc)(int32_t recv_id,
			petuum::ServerPushOpLogIterationMsg* msg, bool is_last);
		// used by server. Msg are should delieved to every client.
		void ServerCreateSendModelSliceMsg(int32_t client_id, ClientModelSliceRequestMsg& slice_request_msg,
			ServerPushModelSliceMsgFunc SendMsg);

	private:
		void GenerateRow() {
			for (int32_t index = 0; index < num_vocabs_; ++index) {
				table_[index] = hybrid_map(
					mem_block_ + dict_[index].offset_,
					dict_[index].is_dense_,
					dict_[index].capacity_,
					rehashing_buf_);
			}
		}

		inline int32_t RowIDToIndex(int32_t row_id) const {
			CHECK(row_id % GlobalContext::get_num_clients() == client_id_);
			return row_id / GlobalContext::get_num_clients();
		}
		inline int32_t IndexToRowID(int32_t index) const {
			// return client_id_ * GlobalContext::get_num_clients() + index;
			return index * GlobalContext::get_num_clients() + client_id_;
		}
		void InitAppendTableToBuffs();
		bool AppendTableToBuffs(int32_t client_id, int32_t* words, int32_t num_word, int32_t& word_index,
			petuum::RecordBuff* buffs, bool resume);
		bool AppendRowToBuffs(int32_t client_id,
			petuum::RecordBuff* buffs,
			const void* row_data, size_t row_size, int32_t row_id);

		LDAModelBlock(const LDAModelBlock &other) = delete;
		LDAModelBlock& operator=(const LDAModelBlock &other) = delete;

	private:
		int32_t client_id_;
		int32_t num_vocabs_;
		WordEntry *dict_;
		int32_t *mem_block_;
		int64_t mem_block_size_;
		// mutable std::mutex mutex_;

		std::vector<hybrid_map> table_;

		StripedLock<int32_t> lock_;

		int32_t *rehashing_buf_;
		// int32_t *num_deleted_key_vector_;

		// for serialization
		static const size_t kSendDeltaMsgSizeInit = 16 * 1024 * 1024;
		static const size_t kTmpRowBuffSizeInit = 64 * 1024;
		int32_t index_iter_; // use index_iter_ to traverse the vector
		size_t send_msg_data_size_;
		size_t tmp_row_buff_size_;
		size_t curr_row_size_;
		uint8_t* tmp_row_buff_;
	};

}