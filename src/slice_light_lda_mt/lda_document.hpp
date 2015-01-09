// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date: 2014.9.25
#pragma once

#include <cstdlib>
#include <vector>
#include <cstdint>
#include <list>
#include <string>
#include <algorithm>
#include <glog/logging.h>

namespace lda {

class Document
{
public:
	Document() :doc_size_(0), slice_cursor_(0)
	{
		doc_topic_counter_buff_.resize(2048);
		std::fill(doc_topic_counter_buff_.begin(), doc_topic_counter_buff_.begin() + 1024, -1);
		std::fill(doc_topic_counter_buff_.begin() + 1024, doc_topic_counter_buff_.end(), 0);
	}
	~Document(){}

	Document(const Document &&other)
	{
		words_ = std::move(other.words_);
		topics_ = std::move(other.topics_);
		doc_topic_counter_buff_ = std::move(other.doc_topic_counter_buff_);
		doc_size_ = other.doc_size_;
		slice_cursor_ = other.slice_cursor_;
	}
	Document& operator=(const Document &&other)
	{
		words_ = std::move(other.words_);
		topics_ = std::move(other.topics_);
		doc_topic_counter_buff_ = std::move(other.doc_topic_counter_buff_);
		doc_size_ = other.doc_size_;
		slice_cursor_ = other.slice_cursor_;
	}

	inline int32_t token_num() const
	{
		return doc_size_;
	}

	inline int32_t word(int idx) const
	{
		return words_[idx];
	}
	inline int32_t topic(int idx) const
	{
		return topics_[idx];
	}


	inline std::vector<int32_t>& get_words()
	{
		return words_;
	}
	inline std::vector<int32_t>& get_topics()
	{
		return topics_;
	}
	inline int32_t& get_cursor()
	{
		return slice_cursor_;
	}

	inline void get_doc_topic_vec(std::vector<int32_t> &doc_topic_vec)
	{
		for (auto topic : topics_)
		{
			++doc_topic_vec[topic];
		}
	}

	inline std::vector<int32_t>& get_doc_topic_counter_buff()
	{
		return doc_topic_counter_buff_;
	}

	void add_word_topics(int32_t word, std::vector<int32_t> &topics)
	{
		if (doc_size_ + topics.size() > 512)
		{
			return;
		}

		for (auto topic : topics)
		{
			words_.push_back(word);
			topics_.push_back(topic);
		}
		doc_size_ += topics.size();
	}

private:
	Document(const Document &other) = delete;
	Document& operator=(const Document &other) = delete;

	std::vector<int32_t> words_;
	std::vector<int32_t> topics_;
	int32_t doc_size_;
	int32_t slice_cursor_;
	std::vector<int32_t> doc_topic_counter_buff_;
};

typedef std::vector<Document> DocBatch;

}  // namespace lda
