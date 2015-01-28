// Author: Gao Fei (v-feigao@microsoft.com)
// Date: 2014.09.30

#pragma once

#include <vector>
#include <unordered_map>
#include <glog/logging.h>
#include "util/dense_row.hpp"

namespace petuum {
	class DeltaArray;
	class SummaryDelta;
	class RecordBuff;
	class ClientSendOpLogIterationMsg;
	class ServerPushOpLogIterationMsg;

	class SummaryRow {
	public:
		SummaryRow():table_id_(2) {}
		SummaryRow(int32_t table_id, int32_t num_topic) : 
			table_id_(table_id), num_topics_(num_topic) {
			summary_row_.Init(num_topics_);
		}
		std::string DebugString() const {
			return summary_row_.DebugString();
		}
	protected:
		DenseRow<int64_t> summary_row_;
		int32_t num_topics_;
    public:
		const int32_t table_id_;
	};

	class ClientSummaryRow : public SummaryRow {
	public:
		ClientSummaryRow() {}
		ClientSummaryRow(int32_t table_id, int32_t num_topic) : 
			SummaryRow(table_id, num_topic) {
			send_msg_data_size_ = kSendDeltaMsgSizeInit;
			tmp_row_buff_size_ = kTmpRowBuffSizeInit;
		}
		int64_t GetSummaryCount(int32_t column_id) const {
			CHECK_GE(summary_row_[column_id], 0);
			return summary_row_[column_id];
		}
		void Reset();
		void ApplyServerModelSliceRequestReply(ServerPushOpLogIterationMsg& msg);
		void ApplyRowOpLog(int32_t table_id, int32_t row_id,
			const void *data, size_t row_size);
		void MergeFrom(const DeltaArray& delta_array);
		void MergeFrom(const SummaryDelta& summary_delta);
		typedef void(*ClientSendTableDeltaMsgFunc)(int32_t recv_id,
			petuum::ClientSendOpLogIterationMsg* msg, bool is_last, bool is_iteration_clock);
		void ClientCreateSendTableDeltaMsg(
			ClientSendTableDeltaMsgFunc SendMsg);
	private:
		void InitAppendTableToBuffs();
		bool AppendTableToBuffs(
			petuum::RecordBuff* buffs, bool resume);
		bool AppendRowToBuffs(
			petuum::RecordBuff* buffs,
			const void* row_data, size_t row_size, int32_t row_id);
	private:
		static const size_t kSendDeltaMsgSizeInit = 16 * 1024 * 1024;
		static const size_t kTmpRowBuffSizeInit = 64 * 1024;
		size_t send_msg_data_size_;
		size_t tmp_row_buff_size_;
		size_t curr_row_size_;
		uint8_t* tmp_row_buff_;
	};

	class ServerSummaryRow : public SummaryRow {
	public:
		ServerSummaryRow(int32_t table_id, int32_t num_topic) :
			SummaryRow(table_id, num_topic) {
			send_msg_data_size_ = kSendDeltaMsgSizeInit;
			tmp_row_buff_size_ = kTmpRowBuffSizeInit;
		}
		void ApplyClientSendOpLogIterationMsg(ClientSendOpLogIterationMsg& msg);
		void ApplyRowOpLog(int32_t table_id, int32_t row_id,
			const void *data, size_t row_size);
		typedef void(*ServerPushModelSliceMsgFunc)(int32_t recv_id,
			petuum::ServerPushOpLogIterationMsg* msg, bool is_last);
		void ServerCreateSendModelSliceMsg(int32_t client_id,
			ServerPushModelSliceMsgFunc SendMsg);

		void Dump(const std::string& dump_file);

	private:
		void InitAppendTableToBuffs();
		bool AppendTableToBuffs(int32_t client_id,
			petuum::RecordBuff* buffs, bool resume);
		bool AppendRowToBuffs(int32_t client_id,
			petuum::RecordBuff* buffs,
			const void* row_data, size_t row_size, int32_t row_id);
	private:
		mutable std::mutex mutex_;
		static const size_t kSendDeltaMsgSizeInit = 16 * 1024 * 1024;
		static const size_t kTmpRowBuffSizeInit = 512;
		size_t send_msg_data_size_;
		size_t tmp_row_buff_size_;
		size_t curr_row_size_;
		uint8_t* tmp_row_buff_;
	};
}
