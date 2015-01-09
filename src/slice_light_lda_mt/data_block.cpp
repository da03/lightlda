#include "data_block.h"
#include <glog/logging.h>

namespace lda {
	LDADataBlock::LDADataBlock(std::string file_name, int32_t num_threads) : 
		file_name_(file_name), num_threads_(num_threads), has_read_(false) {}

	LDADataBlock::~LDADataBlock() {
		if (has_read_) {
			delete[] offset_buffer_;
			delete[] documents_buffer_;
		}
	}

	void LDADataBlock::Read() {
		LOG(INFO) << "load block file " << file_name_;
		std::ifstream block_file(file_name_, std::ios::in | std::ios::binary);
		CHECK(block_file.good()) << "Fails to open file: " << file_name_;

		block_file.read(reinterpret_cast<char*>(&num_document_), sizeof(int32_t));
		offset_buffer_ = new int64_t[num_document_ + 2]; // +2: one for the end of last document,
														 //     one for the end of local vocabs;
		CHECK(offset_buffer_) << "Memory allocation fails: offset_buffer";
		block_file.read(reinterpret_cast<char*>(offset_buffer_), 
			sizeof(int64_t) * (num_document_ + 2)); // +2: see above

		LOG(INFO) << "# of docs: " << num_document_;
				
		corpus_size_ = offset_buffer_[num_document_];
		documents_buffer_ = new int32_t[corpus_size_];
		CHECK(documents_buffer_) << "Memory allocation fails: documents_buffer";
		block_file.read(reinterpret_cast<char*>(documents_buffer_), 
			sizeof(int32_t)* corpus_size_);
		
		LOG(INFO) << "# of tokens (roughly): " << corpus_size_ / 2;
			
		has_read_ = true;
		block_file.close();
	}

	void LDADataBlock::Write() {
		LOG(INFO) << "save block file " << file_name_;
		std::ofstream block_file(file_name_, std::ios::out | std::ios::binary);
		CHECK(block_file.good()) << "Fails to open file: " << file_name_;
		// write the number of docs in this block
		block_file.write(reinterpret_cast<char*>(&num_document_), sizeof(int32_t));
		// write the length of doc and vocabs in this block
		block_file.write(reinterpret_cast<char*>(offset_buffer_), sizeof(int64_t) * (num_document_ + 2));
		// write the documents word topic assignment
		block_file.write(reinterpret_cast<char*>(documents_buffer_), sizeof(int32_t)* (corpus_size_));
		block_file.close();
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

	std::shared_ptr<LDADocument> LDADataBlock::GetOneDoc(int32_t index) {
		CHECK(has_read_) << "Invalid data block";
		CHECK(index < num_document_);
		std::shared_ptr<LDADocument> returned_ptr(
			new LDADocument(documents_buffer_ + offset_buffer_[index],
							documents_buffer_ + offset_buffer_[index + 1]
			)
		);
		return returned_ptr;
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
			if (++num == 512)
				return;
		}
	}
}