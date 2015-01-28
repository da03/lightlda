// Author: Gao Fei (v-feigao@microsoft.com)
// Date: 2014.10.15

#include "memory/delta_slice.h"
#include "memory/local_vocab.h"
#include "system/ps_msgs.hpp"
#include "util/delta_table.h"

namespace lda {

	DeltaSlice::DeltaSlice() {
		util::Context& context = util::Context::get_instance();
		// 
		memory_block_size_ = context.get_int64("delta_max_capacity");

		try{
			memory_block_ = new int32_t[memory_block_size_];
		}
		catch (std::bad_alloc& ba) {
			LOG(FATAL) << "Bad Alloc caught: " << ba.what();
		}

		int32_t V = context.get_int32("num_vocabs");

		table_.resize(V);
		int32_t K = context.get_int32("num_topics");

		num_delta_threads_ = context.get_int32("num_delta_threads");
		rehashing_buf_.resize(num_delta_threads_);
		for (auto& buf : rehashing_buf_)
			buf = new int32_t[2 * K];
		// rehashing_buf_ = new int32_t[2 * K];

		send_msg_data_size_ = kSendDeltaMsgSizeInit;
		tmp_row_buff_size_ = kTmpRowBuffSizeInit;
	}
	DeltaSlice::~DeltaSlice() {
		delete[] memory_block_;
		for (auto& buf : rehashing_buf_) {
			delete[] buf;
			buf = nullptr;
		}
		// delete[] rehashing_buf_;
	}

	// Must Init before called other method
	void DeltaSlice::Init(LocalVocab* local_vocab, int32_t slice_id) {
		CHECK(local_vocab != nullptr);
		local_vocab_ = local_vocab;
		slice_id_ = slice_id;
		//std::fill(memory_block_, memory_block_ + memory_block_size_, 0);
		memset(memory_block_, 0, memory_block_size_ * sizeof(int32_t));
		GenerateRow();
	}

	void DeltaSlice::MergeFrom(const petuum::DeltaArray& other) {

		for (int i = 0; i < other.index_; ++i) {
			Update(other.array_[i].word_id, other.array_[i].topic_id, other.array_[i].delta);
		}
	}

	void DeltaSlice::Update(int32_t word, int32_t topic, int32_t update) {
		// hybrid_map row = GetRow(word);
		// row.inc(topic, update);
		int32_t index = local_vocab_->WordToIndex(slice_id_, word);
		table_[index].inc(topic, update);
	}

	void DeltaSlice::GenerateRow() {
		int32_t size = local_vocab_->SliceSize(slice_id_);
		SliceMeta& dict = local_vocab_->Meta(slice_id_);

		for (int32_t index = 0; index < size; ++index) {
			int32_t word = local_vocab_->IndexToWord(slice_id_, index);
			table_[index] = lda::hybrid_map(memory_block_ + dict[index].delta_offset_,
				dict[index].is_delta_dense_,
				dict[index].delta_capacity_, 
				rehashing_buf_[word % num_delta_threads_]);
		}
	}

	int64_t DeltaSlice::ClientCreateSendTableDeltaMsg(
		ClientSendDeltaMsgFunc SendMsg,
		bool is_iteration_clock) {

		nonzero_entries_ = 0;

		std::vector<int32_t>& server_ids = petuum::GlobalContext::get_server_ids();
		std::unordered_map<int32_t, petuum::RecordBuff> buffs;
		std::unordered_map<int32_t, petuum::ClientSendOpLogIterationMsg*> msg_map;

		// create a message for each server
		for (int i = 0; i < petuum::GlobalContext::get_num_servers(); ++i) {
			int32_t server_id = server_ids[i];
			petuum::ClientSendOpLogIterationMsg* msg = new petuum::ClientSendOpLogIterationMsg(send_msg_data_size_);
			msg->get_server_id() = server_id;
			msg->get_table_id() = petuum::GlobalContext::kWordTopicTableID;
			msg_map[server_id] = msg;
			buffs.insert(std::make_pair(server_id, petuum::RecordBuff(msg->get_data(), send_msg_data_size_)));
		}
		// VLOG(0) << "Serializing table " << table_id_;
		// set table id
		for (auto server_id : server_ids) {
			petuum::RecordBuff& record_buff = buffs[server_id];
			int32_t* table_id_ptr = record_buff.GetMemPtrInt32();
			if (table_id_ptr == 0) {
				//VLOG(0) << "Not enough space for table id, sent out to " << server_id;
				SendMsg(server_id, msg_map[server_id], false, false);
				memset((msg_map[server_id])->get_data(), 0, send_msg_data_size_);
				record_buff.ResetOffset();
				table_id_ptr = record_buff.GetMemPtrInt32();
			}
			*table_id_ptr = petuum::GlobalContext::kWordTopicTableID;
		}
		int failed_server_id;
		// DeltaTable packs the data
		// InitAppendTableToBuffs();
		index_iter_ = 0;
		tmp_row_buff_ = new uint8_t[tmp_row_buff_size_];
		VLOG(0) << "Begin serialize delta slice";
		bool pack_suc = AppendTableToBuffs(&buffs, failed_server_id, false);
		while (!pack_suc) {
			//VLOG(0) << "Not enough space for appending row, send out to " << failed_server_id;
			petuum::RecordBuff &record_buff = buffs[failed_server_id];
			int32_t* buff_end_ptr = record_buff.GetMemPtrInt32();
			if (buff_end_ptr != 0) {
				*buff_end_ptr = petuum::GlobalContext::get_serialized_table_end();
			}

			SendMsg(failed_server_id, msg_map[failed_server_id], false, false);
			memset((msg_map[failed_server_id])->get_data(), 0, send_msg_data_size_);
			record_buff.ResetOffset();
			int32_t* table_id_ptr = record_buff.GetMemPtrInt32();
			*table_id_ptr = petuum::GlobalContext::kWordTopicTableID;
			pack_suc = AppendTableToBuffs(&buffs, failed_server_id, true);
		}
		// only one table
		for (auto server_id : server_ids) {
			petuum::RecordBuff& record_buff = buffs[server_id];
			int32_t* table_end_ptr = record_buff.GetMemPtrInt32();
			if (table_end_ptr == 0) {
				//VLOG(0) << "Not enough space for table end, send out to " << server_id;
				SendMsg(server_id, msg_map[server_id], true, is_iteration_clock);
				continue;
			}
			*table_end_ptr = petuum::GlobalContext::get_serialized_table_end();
			msg_map[server_id]->get_avai_size() = buffs[server_id].GetMemUsedSize();
			SendMsg(server_id, msg_map[server_id], true, is_iteration_clock);
			delete msg_map[server_id];
		}

		return nonzero_entries_;
	}

	bool DeltaSlice::AppendTableToBuffs(
		std::unordered_map<int32_t, petuum::RecordBuff>* buffs,
		int32_t& failed_server_id, bool resume) {
		if (resume) {
			int32_t row_id = local_vocab_->IndexToWord(slice_id_, index_iter_);
			bool append_row_suc = AppendRowToBuffs(buffs,
				tmp_row_buff_, curr_row_size_, row_id, failed_server_id);
			if (!append_row_suc) return false;
			++index_iter_;
		}
		for (; index_iter_ != local_vocab_->SliceSize(slice_id_); ++index_iter_) {
			int32_t row_id = local_vocab_->IndexToWord(slice_id_, index_iter_);
			lda::hybrid_map& row = table_[index_iter_];
			curr_row_size_ = row.SerializedSize();
			nonzero_entries_ += curr_row_size_ / (sizeof(int32_t)+sizeof(int32_t));
			if (curr_row_size_ == 0) continue;
			if (curr_row_size_ > tmp_row_buff_size_) {
				delete[] tmp_row_buff_;
				tmp_row_buff_size_ = curr_row_size_;
				tmp_row_buff_ = new uint8_t[curr_row_size_];
			}
			curr_row_size_ = row.Serialize(tmp_row_buff_);
			bool append_row_suc = AppendRowToBuffs(buffs,
				tmp_row_buff_, curr_row_size_, row_id, failed_server_id);
			if (!append_row_suc) {
				// VLOG(0) << "Failed at row " << row_id << ". Slice id = " << slice_id_;
				return false;
			}
		}
		delete[] tmp_row_buff_;
		return true;
	}

	bool DeltaSlice::AppendRowToBuffs(
		std::unordered_map<int32_t, petuum::RecordBuff>* buffs,
		const void* row_data, size_t row_size, int32_t row_id, int32_t& failed_server_id) {
		int32_t server_id = petuum::GlobalContext::GetRowPartitionServerID(row_id);
		failed_server_id = server_id;
		return (*buffs)[server_id].Append(row_id, row_data, row_size);
	}

}