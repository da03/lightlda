// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date: 2014.9.25
#pragma once

#include <cstdlib>
#include <vector>
#include <cstdint>
#include <list>
#include <string>
#include <glog/logging.h>

namespace lda {

class Document
{
public:
	Document(){}
	~Document(){}

	Document(const Document &&other)
	{
		words_ = std::move(other.words_);
		topics_ = std::move(other.topics_);
	}
	Document& operator=(const Document &&other)
	{
		words_ = std::move(other.words_);
		topics_ = std::move(other.topics_);
	}

	inline int32_t token_num() const
	{
		return words_.size();
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
	inline void get_doc_topic_vec(std::vector<int32_t> &doc_topic_vec)
	{
		for (auto topic : topics_)
		{
			++doc_topic_vec[topic];
		}
	}

	void add_word_topics(int32_t word, std::vector<int32_t> &topics)
	{
		for (auto topic : topics)
		{
			words_.push_back(word);
			topics_.push_back(topic);
		}
	}

private:
	Document(const Document &other) = delete;
	Document& operator=(const Document &other) = delete;

	std::vector<int32_t> words_;
	std::vector<int32_t> topics_;
};

typedef std::vector<Document> DocBatch;

}  // namespace lda
