// Author: Gao Fei (v-feigao@microsoft.com)
// Date: 2014.09.30

#include "memory/summary_row.hpp"
#include <fstream>
#include "system/ps_msgs.hpp"
#include "util/delta_table.h"
#include "util/record_buff.hpp"
#include "util/serialized_row_reader.hpp"

namespace petuum {

	void ClientSummaryRow::Reset() {
		summary_row_.Clear();
	}

	void ClientSummaryRow::ApplyServerModelSliceRequestReply(
		ServerPushOpLogIterationMsg& msg) {
		int32_t table_id = msg.get_table_id();
		CHECK_EQ(msg.get_table_id(), GlobalContext::kSummaryRowID);
		SerializedRowReader row_reader(msg.get_data(), msg.get_avai_size());
		bool to_read = row_reader.Restart();
		int32_t row_id;
		size_t row_size;
		if (to_read) {
			const void *data = row_reader.Next(&table_id, &row_id, &row_size);
			while (data != NULL) {
				CHECK_EQ(row_id, 0);
				ApplyRowOpLog(table_id, row_id, data, row_size);
				data = row_reader.Next(&table_id, &row_id, &row_size);
			}
		}
	}

	void ClientSummaryRow::ApplyRowOpLog(int32_t table_id, int32_t row_id,
		const void *data, size_t row_size) {
		summary_row_.ApplySparseBatchIncUnsafe(data, row_size);
	}

	void ClientSummaryRow::InitAppendTableToBuffs() {
		tmp_row_buff_ = new uint8_t[tmp_row_buff_size_];
	}

	void ClientSummaryRow::ClientCreateSendTableDeltaMsg(
		ClientSendTableDeltaMsgFunc SendMsg) {

		/*petuum::ServerPushOpLogIterationMsg* msg =
			new petuum::ServerPushOpLogIterationMsg(send_msg_data_size_);*/
		petuum::ClientSendOpLogIterationMsg* msg = new petuum::ClientSendOpLogIterationMsg(send_msg_data_size_);
		petuum::RecordBuff record_buff(msg->get_data(), send_msg_data_size_);

		VLOG(0) << "Client Create send summary row delta msg " << table_id_;
		int32_t server_id = GlobalContext::get_server_ids()[0];;
		msg->get_server_id() = GlobalContext::get_server_ids()[0];
		msg->get_table_id() = table_id_;
		int32_t *table_id_ptr = record_buff.GetMemPtrInt32();
		if (table_id_ptr == 0) {
			VLOG(0) << "Not enough space for table id, send out to " << server_id;
			SendMsg(server_id, msg, false, false);
			memset(msg->get_data(), 0, send_msg_data_size_);
			record_buff.ResetOffset();
			table_id_ptr = record_buff.GetMemPtrInt32();
		}
		*table_id_ptr = table_id_;
		InitAppendTableToBuffs();
		bool pack_suc = AppendTableToBuffs(&record_buff, false);
		while (!pack_suc) {
			int32_t* buff_end_ptr = record_buff.GetMemPtrInt32();
			if (buff_end_ptr != 0)
				*buff_end_ptr = petuum::GlobalContext::get_serialized_table_end();
			SendMsg(server_id, msg, false, false);
			memset(msg->get_data(), 0, send_msg_data_size_);
			record_buff.ResetOffset();
			int32_t* table_id_ptr = record_buff.GetMemPtrInt32();
			*table_id_ptr = table_id_;
			pack_suc = AppendTableToBuffs(&record_buff, true);
		}

		int32_t* table_end_ptr = record_buff.GetMemPtrInt32();
		if (table_end_ptr == 0) {
			SendMsg(server_id, msg, true, false);
		}
		else {
			*table_end_ptr = petuum::GlobalContext::get_serialized_table_end();
			msg->get_avai_size() = record_buff.GetMemUsedSize();
			SendMsg(server_id, msg, true, false);
		}
		delete msg;
	}


	bool ClientSummaryRow::AppendTableToBuffs(
		petuum::RecordBuff* buffs, bool resume) {
		if (resume) {
			bool append_row_suc = AppendRowToBuffs(buffs, tmp_row_buff_, curr_row_size_, 0);
			if (!append_row_suc) return false;
			//++index_iter_;
		}
		//for (; index_iter_ != num_words_; ++index_iter_) {
		curr_row_size_ = summary_row_.SparseSerializedSize();//SerializedSize();
		if (curr_row_size_ > tmp_row_buff_size_) {
			delete[] tmp_row_buff_;
			tmp_row_buff_size_ = curr_row_size_;
			tmp_row_buff_ = new uint8_t[curr_row_size_];
		}
		VLOG(0) << "Serialize summary row " << " row size = " << curr_row_size_;
		curr_row_size_ = summary_row_.SparseSerialize(tmp_row_buff_);//->Serialize(tmp_row_buff_);
		VLOG(0) << "Sucecss serialize summary row " << " row size = " << curr_row_size_;
		bool append_row_suc = AppendRowToBuffs(buffs, tmp_row_buff_, curr_row_size_, 0);
		if (!append_row_suc) {
			VLOG(0) << "Failed at Summary Row";
			return false;
		}
		//}
		delete[] tmp_row_buff_;
		return true;
	}

	bool ClientSummaryRow::AppendRowToBuffs(
		petuum::RecordBuff* buffs,
		const void* row_data, size_t row_size, int32_t row_id) {
		//int32_t bg_id = petuum::GlobalContext::get_head_bg_id(client_id);
		bool suc = buffs->Append(row_id, row_data, row_size);
		return suc;
	}

	void ClientSummaryRow::MergeFrom(const DeltaArray& other) {
		for (int i = 0; i < other.index_; ++i) {
			//CHECK_LE(summary_row_[other.array_[i].topic_id], 0x7FFFFFFFFFFFFFFE);
			// Note(v-feigao): int32_t to int64_t
			int64_t delta_value = other.array_[i].delta;
			summary_row_.ApplyIncUnsafe(other.array_[i].topic_id, &delta_value);
		}
	}

	void ClientSummaryRow::MergeFrom(const SummaryDelta& summary_delta) {
		const int32_t* delta = summary_delta.delta_;
		for (int i = 0; i < summary_delta.K_; ++i) {
			if (delta[i] != 0) {
				// Note(v-feigao): int32_t to int64_t
				int64_t delta_value = delta[i];
				summary_row_.ApplyIncUnsafe(i, &delta_value);
			}
		}
	}

	void ServerSummaryRow::Dump(const std::string& dump_file) {
		std::ofstream fout(dump_file);

		fout << summary_row_.DebugString() << std::endl;
	}

	void ServerSummaryRow::ApplyClientSendOpLogIterationMsg(
		ClientSendOpLogIterationMsg& msg) {
		int32_t table_id = msg.get_table_id();
		CHECK_EQ(table_id, table_id_) << "wrong table id ";
		SerializedRowReader row_reader(msg.get_data(), msg.get_avai_size());
		bool to_read = row_reader.Restart();
		int32_t row_id;
		size_t row_size;
		if (to_read) {
			const void *data = row_reader.Next(&table_id, &row_id, &row_size);
			while (data != NULL) {
				// TODO(v-feigao): modified with Copy-on-write.
				std::lock_guard<std::mutex> lock_guard(mutex_);
				ApplyRowOpLog(table_id, row_id, data, row_size);
				data = row_reader.Next(&table_id, &row_id, &row_size);
			}
		}
	}

	void ServerSummaryRow::ApplyRowOpLog(int32_t table_id,
		int32_t row_id, const void *data, size_t row_size) {
		summary_row_.ApplySparseBatchIncUnsafe(data, row_size);
	}

	void ServerSummaryRow::ServerCreateSendModelSliceMsg(int32_t client_id,
		ServerPushModelSliceMsgFunc SendMsg) {
		petuum::ServerPushOpLogIterationMsg* msg =
			new petuum::ServerPushOpLogIterationMsg(send_msg_data_size_);
		petuum::RecordBuff record_buff(msg->get_data(), send_msg_data_size_);

		VLOG(0) << "Server Serializing table " << table_id_;
		int32_t head_bg_id = petuum::GlobalContext::get_head_bg_id(client_id);
		msg->get_bg_id() = head_bg_id;
		msg->get_table_id() = table_id_;
		int32_t *table_id_ptr = record_buff.GetMemPtrInt32();
		if (table_id_ptr == 0) {
			VLOG(0) << "Not enough space for table id, send out to " << head_bg_id;
			SendMsg(head_bg_id, msg, false);
			memset(msg->get_data(), 0, send_msg_data_size_);
			record_buff.ResetOffset();
			table_id_ptr = record_buff.GetMemPtrInt32();
		}
		CHECK_EQ(table_id_, GlobalContext::kSummaryRowID);
		*table_id_ptr = table_id_;
		InitAppendTableToBuffs();
		bool pack_suc = AppendTableToBuffs(client_id, &record_buff, false);
		while (!pack_suc) {
			int32_t* buff_end_ptr = record_buff.GetMemPtrInt32();
			if (buff_end_ptr != 0)
				*buff_end_ptr = petuum::GlobalContext::get_serialized_table_end();
			SendMsg(head_bg_id, msg, false);
			memset(msg->get_data(), 0, send_msg_data_size_);
			record_buff.ResetOffset();
			int32_t* table_id_ptr = record_buff.GetMemPtrInt32();
			*table_id_ptr = table_id_;
			pack_suc = AppendTableToBuffs(client_id, &record_buff, true);
		}

		int32_t* table_end_ptr = record_buff.GetMemPtrInt32();
		if (table_end_ptr == 0) {
			SendMsg(head_bg_id, msg, true);
		}
		else {
			*table_end_ptr = petuum::GlobalContext::get_serialized_table_end();
			msg->get_avai_size() = record_buff.GetMemUsedSize();
			SendMsg(head_bg_id, msg, true);
		}
		delete msg;
	}


	void ServerSummaryRow::InitAppendTableToBuffs() {
		tmp_row_buff_ = new uint8_t[tmp_row_buff_size_];
	}

	bool ServerSummaryRow::AppendTableToBuffs(int32_t client_id, 
		petuum::RecordBuff* buffs, bool resume) {
		std::lock_guard<std::mutex> lock_guard(mutex_);
		if (resume) {
			bool append_row_suc = AppendRowToBuffs(client_id, buffs, tmp_row_buff_, curr_row_size_, 0);
			if (!append_row_suc) return false;
			//++index_iter_;
		}
		//for (; index_iter_ != num_words_; ++index_iter_) {
			curr_row_size_ = summary_row_.SparseSerializedSize();//SerializedSize();
			if (curr_row_size_ > tmp_row_buff_size_) {
				delete[] tmp_row_buff_;
				tmp_row_buff_size_ = curr_row_size_;
				tmp_row_buff_ = new uint8_t[curr_row_size_];
			}
			curr_row_size_ = summary_row_.SparseSerialize(tmp_row_buff_);//->Serialize(tmp_row_buff_);
			bool append_row_suc = AppendRowToBuffs(client_id, buffs, tmp_row_buff_, curr_row_size_, 0);
			if (!append_row_suc) {
				VLOG(0) << "Failed at Summary Row";
				return false;
			}
		//}
		delete[] tmp_row_buff_;
		return true;
	}

	bool ServerSummaryRow::AppendRowToBuffs(int32_t client_id,
		petuum::RecordBuff* buffs,
		const void* row_data, size_t row_size, int32_t row_id) {
		int32_t bg_id = petuum::GlobalContext::get_head_bg_id(client_id);
		bool suc = buffs->Append(row_id, row_data, row_size);
		return suc;
	}

}