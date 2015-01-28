// author: Gao Fei(v-feigao@microsoft.com)
// data: 2014-10-02

#pragma once

#include <algorithm>
#include <fstream>
#include <string>
#include <memory>
#include <mutex>
#include <glog/logging.h>
#include "base/common.hpp"
#include "util/light_hash_map.h"

namespace lda {
	class LDADocument;

	class LDADataBlock {
	public:
		LDADataBlock();
		~LDADataBlock();

		void Read(std::string file_name); 
		
		void Write();	

		bool HasRead() const { return has_read_; }
		
		// Return the first document for thread thread_id
		int32_t Begin(int32_t thread_id);

		// Return the next to last document for thread thread_id
		int32_t End(int32_t thread_id);

		std::shared_ptr<LDADocument> GetOneDoc(int32_t index);

	private:
		void GenerateDocument();

	private:
		std::string file_name_;
		int32_t num_threads_;
		bool has_read_; // equal true if LDADataBlock holds memory

		// int32_t* memory_block_;
		int32_t max_num_document_;
		int64_t memory_block_size_;

		std::vector<std::shared_ptr<LDADocument>> documents_;

		int32_t num_document_; 
		int64_t* offset_buffer_; // offset_buffer_ size = num_document_ + 1
		int64_t corpus_size_;
		int32_t* documents_buffer_; // documents_buffer_ size = corpus_size_;
	};

	class LDADocument {
	public:
		const int32_t kMaxSizeLightHash = 512; // This is for the easy use of LightHashMap
		
		LDADocument(int32_t* memory_begin, int32_t* memory_end);
		inline int32_t size() const {
			return (std::min)(static_cast<int32_t>((memory_end_-memory_begin_) / 2), kMaxSizeLightHash);
		}
		inline int32_t& get_cursor() {
			return cursor_;
		}
		inline int32_t Word(int32_t index) const { 
			CHECK(index < size());
			return *(memory_begin_ + 1 + index * 2);  
		}
		inline int32_t Topic(int32_t index) const {
			CHECK(index < size());
			return *(memory_begin_ + 2 + index * 2);
		}
		inline void SetTopic(int32_t index, int32_t topic) {
			CHECK(index < size());
			*(memory_begin_ + 2 + index * 2) = topic;
		}
		// should be called when sweeped over all the tokens in a document
		void ResetCursor(); 
		void GetDocTopicCounter(wood::light_hash_map&);
		std::string DebugString() {
			std::string result;
			for (int i = 0; i < size(); ++i) {
				result += std::to_string(Word(i)) + ":" + std::to_string(Topic(i)) + " ";
			}
			return result;
		}
	private:
		int32_t* memory_begin_;
		int32_t* memory_end_;
		int32_t& cursor_; // cursor_ is reference of *memory_begin_
	};
}