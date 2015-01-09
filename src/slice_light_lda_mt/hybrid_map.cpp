#include "hybrid_map.h"
#include <fstream>
#include <gflags\gflags.h>
#include <glog\logging.h>
#include "utils.hpp"
#include "context.hpp"
#include <malloc.h>

namespace lda
{
	LDAModelBlock::LDAModelBlock()
		: dict_(nullptr),
		num_vocabs_(0),
		mem_block_size_(0),
		mem_block_(nullptr),
		alias_mem_block_size_(0),
		alias_mem_block_(nullptr)
	{
	}
	LDAModelBlock::~LDAModelBlock()
	{
		if (dict_)
		{
			delete[]dict_;
		}
		if (mem_block_)
		{
			delete[]mem_block_;
		}
		
		if (alias_mem_block_)
		{
			delete[]alias_mem_block_;
		}
		
		//_aligned_free(alias_mem_block_);
		//free(alias_mem_block_);
	}
	void LDAModelBlock::Read(const std::string &meta_name)
	{
		std::ifstream meta_fin(meta_name, std::ios::in | std::ios::binary);
		CHECK(meta_fin.good()) << "Can not open file: " << meta_name;

		util::Context& context = util::Context::get_instance();
		int32_t V = context.get_int32("num_vocabs");

		meta_fin.read(reinterpret_cast<char*>(&num_vocabs_), sizeof(int32_t));
		CHECK(V == num_vocabs_) << "num_vocabs_ != FLAGS_num_vocabs: " << num_vocabs_ << " vs. " << V;

		dict_ = new WordEntry[num_vocabs_];
		meta_fin.read(reinterpret_cast<char*>(dict_), sizeof(WordEntry) * num_vocabs_);

		mem_block_size_ = dict_[num_vocabs_ - 1].end_offset_;
		mem_block_ = new int32_t[mem_block_size_];
		
		alias_mem_block_size_ = dict_[num_vocabs_ - 1].alias_end_offset_;
		alias_mem_block_ = new int32_t[alias_mem_block_size_];
		//alias_mem_block_ = (int32_t*)_aligned_malloc(alias_mem_block_size_ * sizeof(int32_t), 64);
		//alias_mem_block_ = (int32_t*)malloc(alias_mem_block_size_);
		
		CHECK(alias_mem_block_ != NULL);
		
		meta_fin.close();

		LOG(INFO) << "mem_block_size = " << mem_block_size_ * 4;
		LOG(INFO) << "alias_mem_block_size = " << alias_mem_block_size_ * 4;
	}
}
