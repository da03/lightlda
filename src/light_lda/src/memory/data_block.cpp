//#include <Windows.h>
#include "lda/context.hpp"
#include "memory/data_block.h"

namespace lda {
	LDADataBlock::LDADataBlock() : has_read_(false) {
		util::Context& context = util::Context::get_instance();
		num_threads_ = context.get_int32("num_worker_threads");
		max_num_document_ = context.get_int32("block_size");
		memory_block_size_ = context.get_int64("block_max_capacity");

		documents_.resize(max_num_document_);

		try{
			offset_buffer_ = new int64_t[max_num_document_];
		}
		catch (std::bad_alloc& ba) {
			LOG(FATAL) << "Bad Alloc caught: " << ba.what();
		}

		try{
			documents_buffer_ = new int32_t[memory_block_size_];
		}
		catch (std::bad_alloc& ba) {
			LOG(FATAL) << "Bad Alloc caught: " << ba.what();
		}
	}

	LDADataBlock::~LDADataBlock() {
		delete[] documents_buffer_;
		delete[] offset_buffer_;
	}

	void LDADataBlock::Read(std::string file_name) {
		file_name_ = file_name;
		LOG(INFO) << "load block file " << file_name_;

		std::ifstream block_file(file_name_, std::ios::in | std::ios::binary);
		CHECK(block_file.good()) << "Fails to open file: " << file_name_;

		block_file.read(reinterpret_cast<char*>(&num_document_), sizeof(int32_t));
		
		CHECK_LT(num_document_, max_num_document_) << "offset buffer is not enough for data_block " << file_name_;

		block_file.read(reinterpret_cast<char*>(offset_buffer_), 
			sizeof(int64_t) * (num_document_ + 1)); 
		
		corpus_size_ = offset_buffer_[num_document_];
		CHECK_LE(corpus_size_, memory_block_size_) << "memory block size if not enough for data_block " << file_name_;
		
		block_file.read(reinterpret_cast<char*>(documents_buffer_),
			sizeof(int32_t)* corpus_size_);
			
		block_file.close();
		
		GenerateDocument();
		has_read_ = true;
	}

	void LDADataBlock::Write() {
		CHECK(has_read_);
		LOG(INFO) << "save block file " << file_name_;

		std::string temp_file = file_name_ + ".temp";

		std::ofstream block_file(temp_file, std::ios::out | std::ios::binary);
		CHECK(block_file.good()) << "Fails to open file: " << file_name_;
		// write the number of docs in this block
		block_file.write(reinterpret_cast<char*>(&num_document_), sizeof(int32_t));
		// write the length of doc and vocabs in this block
		block_file.write(reinterpret_cast<char*>(offset_buffer_), sizeof(int64_t) * (num_document_ + 1));
		// write the documents word topic assignment
		block_file.write(reinterpret_cast<char*>(documents_buffer_), sizeof(int32_t)* (corpus_size_));
		block_file.flush();
		block_file.close();

		// Atomic update disk file
		// ORIG: MoveFileExA(temp_file.c_str(), file_name_.c_str(), MOVEFILE_REPLACE_EXISTING);
        if (rename(temp_file.c_str(), file_name_.c_str())==-1) {
            LOG(FATAL) << "Moving file failed!";
        }
		has_read_ = false;
	}

	int32_t LDADataBlock::Begin(int32_t thread_id) {
		int32_t num_of_one_doc = num_document_ / num_threads_;
		return thread_id * num_of_one_doc;
	}

	int32_t LDADataBlock::End(int32_t thread_id) {
		if (thread_id == num_threads_ - 1) // last thread
			return num_document_;
		int32_t num_of_one_doc = num_document_ / num_threads_;
		return (thread_id + 1) * num_of_one_doc;
	}

	void LDADataBlock::GenerateDocument() {
		for (int index = 0; index < num_document_; ++index) {
			documents_[index].reset(
				new LDADocument(documents_buffer_ + offset_buffer_[index],
				documents_buffer_ + offset_buffer_[index + 1]));
		}
	}

	std::shared_ptr<LDADocument> LDADataBlock::GetOneDoc(int32_t index) {
		CHECK(has_read_) << "Invalid data block";
		CHECK(index < num_document_);
		return documents_[index];
	}

	LDADocument::LDADocument(int32_t* memory_begin, int32_t* memory_end) :
		memory_begin_(memory_begin), memory_end_(memory_end), cursor_(*memory_begin) {}

	void LDADocument::ResetCursor() {
		cursor_ = 0;
	}

	void LDADocument::GetDocTopicCounter(wood::light_hash_map& doc_topic_counter) {
		int32_t* p = memory_begin_ + 2;
		int32_t num = 0;
		while (p < memory_end_) {
			doc_topic_counter.inc(*p, 1);
			++p; ++p;
			if (++num == kMaxSizeLightHash)
				return;
		}
	}
}
