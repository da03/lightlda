#include "hybrid_map.h"
#include <fstream>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include "utils.hpp"

#include "util/serialized_row_reader.hpp"


namespace lda
{
	size_t hybrid_map::SerializedSize() const {
		return nonzero_num() * (sizeof(int32_t)+sizeof(int32_t));
	}

	size_t hybrid_map::Serialize(void* bytes) const {
		size_t size = 0;
		CHECK(bytes != NULL) << "Invalid pointer";
		void* data_ptr = bytes;
		if (is_dense_) {
			for (int32_t i = 0; i < capacity_; ++i) {
				//if (memory_[i] > 0) {
				if (memory_[i] != 0) {
					int32_t* col_ptr = reinterpret_cast<int32_t*>(data_ptr);
					*col_ptr = i;
					++col_ptr;
					int32_t* val_ptr = reinterpret_cast<int32_t*>(col_ptr);
					*val_ptr = memory_[i];
					data_ptr = reinterpret_cast<void*>(++val_ptr);
					size += (sizeof(int32_t)+sizeof(int32_t));
				}
			}
			return size;
		}
		else {
			for (int32_t i = 0; i < capacity_; ++i) {
				if (key_[i] > 0) {
					int32_t* col_ptr = reinterpret_cast<int32_t*>(data_ptr);
					*col_ptr = key_[i]-1;
					++col_ptr;
					int32_t* val_ptr = reinterpret_cast<int32_t*>(col_ptr);
					*val_ptr = value_[i];
					data_ptr = reinterpret_cast<void*>(++val_ptr);
					size += (sizeof(int32_t)+sizeof(int32_t));
				}
			}
			return size;
		}
		return size;
	}

	void hybrid_map::ApplySparseBatchInc(const void* data, size_t num_bytes) {
		int32_t num_bytes_per_entry = (sizeof(int32_t)+sizeof(int32_t));
		CHECK_EQ(0, num_bytes % num_bytes_per_entry) << "num_bytes = " << num_bytes;

		int32_t num_entries = num_bytes / num_bytes_per_entry;

		const uint8_t* data_ptr = reinterpret_cast<const uint8_t*>(data);
		for (int i = 0; i < num_entries; ++i) {
			const int32_t* col_ptr = reinterpret_cast<const int32_t*>(data_ptr);
			int32_t col_id = *col_ptr;

			++col_ptr;
			const int32_t* val_ptr = reinterpret_cast<const int32_t*>(col_ptr);
			int32_t val = *val_ptr;
			data_ptr = reinterpret_cast<const uint8_t*>(++val_ptr);
			this->inc(col_id, val);
			//CHECK_GE(data_[col_id], V(0));
		}
		// VLOG(0) << DebugString();
	}

	//LDAModelBlock::LDAModelBlock(int32_t client_id) :
	//	client_id_(client_id),
	//	dict_(nullptr),
	//	num_vocabs_(0),
	//	mem_block_size_(0),
	//	mem_block_(nullptr) {
	//	send_msg_data_size_ = kSendDeltaMsgSizeInit;
	//	tmp_row_buff_size_ = kTmpRowBuffSizeInit;
	//	int32_t num_topics = petuum::GlobalContext::get_num_topics();
	//	rehashing_buf_ = new int32_t[2 * num_topics];
	//}
	//LDAModelBlock::~LDAModelBlock()
	//{
	//	if (dict_)
	//	{
	//		delete[]dict_;
	//	}
	//	if (mem_block_)
	//	{
	//		delete[]mem_block_;
	//	}
	//	if (num_deleted_key_vector_) 
	//	{
	//		delete[]num_deleted_key_vector_;
	//	}
	//	delete[] rehashing_buf_;
	//}
	//void LDAModelBlock::Read(const std::string &meta_name)
	//{
	//	std::ifstream meta_fin(meta_name, std::ios::in | std::ios::binary);
	//	CHECK(meta_fin.good()) << "Can not open file: " << meta_name;

	//	//int32_t V = petuum::GlobalContext::get_num_vocabs();

	//	meta_fin.read(reinterpret_cast<char*>(&num_vocabs_), sizeof(int32_t));
	//	// CHECK(V == num_vocabs_) << "num_vocabs_ != FLAGS_num_vocabs: " << num_vocabs_ << " vs. " << V;

	//	dict_ = new WordEntry[num_vocabs_];
	//	meta_fin.read(reinterpret_cast<char*>(dict_), sizeof(WordEntry) * num_vocabs_);

	//	mem_block_size_ = dict_[num_vocabs_ - 1].end_offset_;
	//	mem_block_ = new int32_t[mem_block_size_];
	//	VLOG(0) << "Read server model block meta data, num_vocabs = " << num_vocabs_ 
	//		<< ". allocate memory size = " << mem_block_size_;

	//	num_deleted_key_vector_ = new int32_t[num_vocabs_];
	//	std::fill(num_deleted_key_vector_, num_deleted_key_vector_ + num_vocabs_, 0);

	//	/*
	//	for (int i = 0; i < num_vocabs_; ++i)
	//	{
	//		CHECK(dict_[i].is_dense_) << "is_dense == 0";
	//	}
	//	*/

	//	meta_fin.close();
	//}

	//void LDAModelBlock::Dump(const std::string& dump_file) {
	//	std::ofstream fout(dump_file);

	//	for (int32_t index = 0; index < num_vocabs_; ++index) {
	//		int32_t word_id = IndexToRowID(index);
	//		fout << word_id << " ";
	//		{
	//			std::lock_guard<std::mutex> lock_guard(mutex_);
	//			hybrid_map row = get_row(word_id);
	//			fout << row.DumpString();
	//		}
	//		fout << std::endl;
	//	}
	//}

	//void LDAModelBlock::ApplyClientSendOpLogIterationMsg(
	//	ClientSendOpLogIterationMsg& msg) {
	//	int32_t table_id;
	//	SerializedRowReader row_reader(msg.get_data(), msg.get_avai_size());
	//	bool to_read = row_reader.Restart();
	//	int32_t row_id;
	//	size_t row_size;
	//	if (to_read) {
	//		const void *data = row_reader.Next(&table_id, &row_id, &row_size);
	//		while (data != NULL) {
	//			// TODO(v-feigao): modified with Copy-on-write.
	//			std::lock_guard<std::mutex> lock_guard(mutex_);
	//			ApplyRowOpLog(table_id, row_id, data, row_size);
	//			data = row_reader.Next(&table_id, &row_id, &row_size);
	//		}
	//	}
	//}

	//void LDAModelBlock::ApplyRowOpLog(int32_t table_id, int32_t row_id,
	//	const void *data, size_t row_size) {
	//	hybrid_map row = get_row(row_id);
	//	row.ApplySparseBatchInc(data, row_size);
	//}

	//void LDAModelBlock::ServerCreateSendModelSliceMsg(int32_t client_id, 
	//	ClientModelSliceRequestMsg& slice_request_msg,
	//	ServerPushModelSliceMsgFunc SendMsg) {

	//	petuum::ServerPushOpLogIterationMsg* msg =
	//		new petuum::ServerPushOpLogIterationMsg(send_msg_data_size_);
	//	petuum::RecordBuff record_buff(msg->get_data(), send_msg_data_size_);

	//	int32_t head_bg_id = petuum::GlobalContext::get_head_bg_id(client_id);
	//	msg->get_bg_id() = head_bg_id;
	//	msg->get_table_id() = GlobalContext::kWordTopicTableID;
	//	int32_t *table_id_ptr = record_buff.GetMemPtrInt32();
	//	if (table_id_ptr == 0) {
	//		//VLOG(0) << "Not enough space for table id, send out to " << head_bg_id;
	//		SendMsg(head_bg_id, msg, false);
	//		memset(msg->get_data(), 0, send_msg_data_size_);
	//		record_buff.ResetOffset();
	//		table_id_ptr = record_buff.GetMemPtrInt32();
	//	}
	//	*table_id_ptr = GlobalContext::kWordTopicTableID;
	//	void* bytes = slice_request_msg.get_data();
	//	int32_t* words = reinterpret_cast<int32_t*>(bytes);
	//	int32_t num_words = *words;
	//	words++;
	//	CHECK_EQ((num_words + 1) * sizeof(int32_t), slice_request_msg.get_avai_size()) << "num of requested word = " << num_words;
	//	int32_t request_word_index = 0;
	//	InitAppendTableToBuffs();

	//	bool pack_suc = AppendTableToBuffs(client_id, words, num_words, request_word_index, &record_buff, false);
	//	while (!pack_suc) {
	//		int32_t* buff_end_ptr = record_buff.GetMemPtrInt32();
	//		if (buff_end_ptr != 0)
	//			*buff_end_ptr = petuum::GlobalContext::get_serialized_table_end();
	//		SendMsg(head_bg_id, msg, false);
	//		memset(msg->get_data(), 0, send_msg_data_size_);
	//		record_buff.ResetOffset();
	//		int32_t* table_id_ptr = record_buff.GetMemPtrInt32();
	//		*table_id_ptr = GlobalContext::kWordTopicTableID;
	//		pack_suc = AppendTableToBuffs(client_id, words, num_words, request_word_index, &record_buff, true);
	//	}

	//	int32_t* table_end_ptr = record_buff.GetMemPtrInt32();
	//	if (table_end_ptr == 0) {
	//		SendMsg(head_bg_id, msg, true);
	//	}
	//	else {
	//		*table_end_ptr = petuum::GlobalContext::get_serialized_table_end();
	//		msg->get_avai_size() = record_buff.GetMemUsedSize();
	//		SendMsg(head_bg_id, msg, true);
	//	}
	//	delete msg;
	//}

	//void LDAModelBlock::InitAppendTableToBuffs() {
	//	index_iter_ = 0;
	//	tmp_row_buff_ = new uint8_t[tmp_row_buff_size_];
	//}

	//bool LDAModelBlock::AppendTableToBuffs(int32_t client_id, 
	//	int32_t* words, int32_t num_word, int32_t& word_index,
	//	petuum::RecordBuff* buffs, bool resume) {
	//	std::lock_guard<std::mutex> lock_guard(mutex_);
	//	if (resume) {
	//		int32_t row_id = words[word_index];
	//		bool append_row_suc = AppendRowToBuffs(client_id, buffs, tmp_row_buff_, curr_row_size_, row_id);
	//		if (!append_row_suc) return false;
	//		++word_index;
	//	}
	//	for (; word_index != num_word; ++word_index) {
	//		int32_t row_id = words[word_index];
	//		// judge if requested word in current Server ModelBlock
	//		if (row_id % GlobalContext::get_num_clients() != GlobalContext::get_client_id())
	//			continue;
	//		hybrid_map row = get_row(row_id);
	//		curr_row_size_ = row.SerializedSize();//SerializedSize();
	//		if (curr_row_size_ == 0) continue;
	//		if (curr_row_size_ > tmp_row_buff_size_) {
	//			delete[] tmp_row_buff_;
	//			tmp_row_buff_size_ = curr_row_size_;
	//			tmp_row_buff_ = new uint8_t[curr_row_size_];
	//		}
	//		curr_row_size_ = row.Serialize(tmp_row_buff_);//->Serialize(tmp_row_buff_);
	//		bool append_row_suc = AppendRowToBuffs(client_id, buffs, tmp_row_buff_, curr_row_size_, row_id);
	//		if (!append_row_suc) {
	//			//VLOG(0) << "Failed at row " << index_iter_;
	//			return false;
	//		}
	//	}
	//	delete[] tmp_row_buff_;
	//	return true;
	//}

	//bool LDAModelBlock::AppendRowToBuffs(int32_t client_id,
	//	petuum::RecordBuff* buffs,
	//	const void* row_data, size_t row_size, int32_t row_id) {
	//	int32_t bg_id = petuum::GlobalContext::get_head_bg_id(client_id);
	//	bool suc = buffs->Append(row_id, row_data, row_size);
	//	//if (!suc)
	//		//VLOG(0) << "Append Row to Buffs failed at row " << row_id;
	//	return suc;
	//}
}
