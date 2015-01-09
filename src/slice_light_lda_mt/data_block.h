// author: Gao Fei(v-feigao@microsoft.com)
// data: 2014-10-02

#pragma once

#include <fstream>
#include <string>
#include <mutex>
#include <algorithm>
#include <glog/logging.h>
#include <memory>

#include "light_hash_map.h"

namespace lda {
	class LDADocument;

	class LDADataBlock {
	public:
		explicit LDADataBlock(std::string file_name, int32_t num_threads);
		~LDADataBlock();
		void Read(); 
		void Write();

		// Return the first document for thread thread_id
		int32_t Begin(int32_t thread_id);
		// Return the next to last document for thread thread_id
		int32_t End(int32_t thread_id);

		std::shared_ptr<LDADocument> GetOneDoc(int32_t index);
		// void ResetGetDoc() { doc_iter_ = 0; }

	private:
		std::string file_name_;
		int32_t num_threads_;
		bool has_read_; // equal true if LDADataBlock holds memory

		int32_t num_document_; 
		int64_t* offset_buffer_; // offset_buffer_ size = num_document_ + 2
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
			// CHECK(index < size());
			return *(memory_begin_ + 1 + index * 2);  
		}
		inline int32_t Topic(int32_t index) const {
			// CHECK(index < size());
			return *(memory_begin_ + 2 + index * 2);
		}
		inline void SetTopic(int32_t index, int32_t topic) {
			// CHECK(index < size());
			*(memory_begin_ + 2 + index * 2) = topic;
		}
		// should be called when sweeped over all the tokens in a document
		void ResetCursor(); 
		void GetDocTopicCounter(wood::light_hash_map&);
	private:
		int32_t* memory_begin_;
		int32_t* memory_end_;
		int32_t& cursor_; // cursor_ is reference of *memory_begin_
	};
}