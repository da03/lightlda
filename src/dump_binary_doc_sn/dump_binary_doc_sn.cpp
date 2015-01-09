// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date: 2014.09.14

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <stdint.h>
#include <chrono>
#include <algorithm>

#include <glog/logging.h>
#include <gflags/gflags.h>


/*
Input file format:
1, libsvm format. The label and feature vector are separated with tab instead of whitespace
2, we assume there is no empty line
3, it is possible to have empty doc, which is represented only by its label
4, the maximum length of input doc can not exceed 10000000

Output file format:
1, the first 4 byte indicates the number of docs in this block
2, the 4 * (doc_num + 1) bytes indicate the offset of reach doc
an example
3    // there are 3 docs in this block
0    // the offset of the 1-st doc
10   // the offset of the 2-nd doc, with this we know the length of the 1-st doc is 5 = 10/2
16   // the offset of the 3-rd doc, with this we know the length of the 2-nd doc is 3 = (16-10)/2 
24   // with this, we know the length of the 3-rd doc is 4 = (24 - 16)/2
w11 t11 w12 t12 w13 t13 w14 t14 w15 t15     // the token-topic list of the 1-st doc
w21 t21 w22 t22 w23 t23                     // the token-topic list of the 2-nd doc
w31 t31 w32 t32 w33 t33 w34 t34             // the token-topic list of the 3-rd doc
*/

/*
NOTE(jiyuan): we cut off the long docs with a threshold 512. That is, in the output, no 
document contains more than 512 tokens.
*/
DEFINE_string(libsvm_doc, "", "the doc file in libsvm format");
DEFINE_string(binary_doc_dir, "", "the output doc file in binary format");
DEFINE_int32(block_size, -1, "the maximum number of docs in each block, \
							     with the default value as -1, the program will produce a single block");

DEFINE_int32(max_doc_size, 512, "The maximum length of a doc, cutting threshold \
								by default, we cut the document to maximally contain 512 tokens");

int32_t buf_idx_;
int64_t *offset_buf_;      // size: FLAGS_block_size + 1
int32_t *block_buf_;       // size: buf_size

// NOTE(jiyuan): we assume the doc length will not exceed 2000000
const int32_t kMaxTokenNum = 2000000;

// 100 * 2 * 100000, assuming the average doc length is 100, each token needs 2 ints (token, topic), each buf can hold 100000 such documents.
const int32_t kMaxBufSize = 20000000; 

double get_time()
{
	auto start = std::chrono::high_resolution_clock::now();
	auto since_epoch = start.time_since_epoch();
	return std::chrono::duration_cast<std::chrono::duration<double, std::ratio<1, 1>>>(since_epoch).count();
}

void count_doc_num(std::string input_doc, int64_t &doc_num)
{
	doc_num = 0;
	std::fstream input_file(input_doc, std::ios::in);
	CHECK(input_file.good()) << "Fails to open file: " << input_doc;
	std::string line;

	// NOTE(jiyuan): we assume there is no empty line inside the libsvm data
	while (std::getline(input_file, line) && !line.empty())
	{
		++doc_num;
	}
	input_file.close();
}

void calc_block_num(int64_t doc_num, int32_t block_size, int32_t &block_num, std::vector<int32_t> &blocks_size)
{
	if (doc_num % block_size == 0)
	{
		block_num = doc_num / block_size;
		blocks_size.resize(block_num);
		std::fill(blocks_size.begin(), blocks_size.end(), block_size);
	}
	else
	{
		block_num = doc_num / block_size + 1;
		blocks_size.resize(block_num);
		std::fill(blocks_size.begin(), blocks_size.end(), block_size);
		blocks_size.back() = doc_num % block_size;
	}
}

void dump_blocks(std::string input_doc, std::string output_dir, int32_t block_num, std::vector<int32_t> &blocks_size)
{
	//LOG(INFO) << "input_doc = " << input_doc;
	//LOG(INFO) << "output_dir = " << output_dir;
	//LOG(INFO) << "block_num = " << block_num;
	//LOG(INFO) << "blocks_size[0] = " << blocks_size[0];

	std::ifstream input_file(input_doc, std::ios::in);
	CHECK(input_file.good()) << "Fails to open file: " << input_doc;
	
	const char* line = NULL;
	char *endptr = NULL;
	const int kBASE = 10;

	int64_t total_token = 0;

	int doc_buf_idx;
	int32_t *block_buf;

	block_buf = new int32_t[kMaxTokenNum];
	for (int i = 0; i < block_num; ++i)
	{
		std::string block_name = output_dir + "\\block." + std::to_string(i);

		std::ofstream block_file(block_name, std::ios::out | std::ios::binary);
		CHECK(block_file.good()) << "Fails to create file: " << block_name;

		// LOG(INFO) << "block_name = " << block_name;

		int block_size = blocks_size[i];
		std::string str_line;

		memset(offset_buf_, 0, sizeof(int64_t)* (block_size + 1));

		// write the number of docs in this block
		block_file.write(reinterpret_cast<char*> (&block_size), sizeof(int32_t));

		// write the length of each doc in this block
		block_file.write(reinterpret_cast<char*> (offset_buf_), sizeof(int64_t)* (block_size + 1));

		buf_idx_ = 0;
		for (int j = 0; j < block_size; ++j)
		{			
			CHECK(getline(input_file, str_line) && !str_line.empty()) << "Invalid input";
			str_line += '\n';
			line = str_line.c_str();
			int doc_count = 0;

			doc_buf_idx = 0;     // the first int in the buf indicates the |cursor| varaible for model slicing
			block_buf[doc_buf_idx++] = 0;

			strtol(line, &endptr, kBASE);
			char *ptr = endptr;
			while (*ptr == '\t') ++ptr;
			while (*ptr != '\n')
			{
				if (doc_count >= FLAGS_max_doc_size) break;
				// read a word_id:count pair
				int32_t word_id = strtol(ptr, &endptr, kBASE);
				ptr = endptr;
				CHECK_EQ(':', *ptr) << "Invalid input";
				int32_t count = strtol(++ptr, &endptr, kBASE);
				ptr = endptr;

				for (int k = 0; k < count; ++k)
				{
					block_buf[doc_buf_idx++] = word_id;
					block_buf[doc_buf_idx++] = 0;
					++doc_count;
					if (doc_count >= FLAGS_max_doc_size) break;
				}
				while (*ptr == ' ') ++ptr;
			}

			if (buf_idx_ + doc_buf_idx > kMaxBufSize)
			{
				block_file.write(reinterpret_cast<char*> (block_buf_), sizeof(int32_t)* buf_idx_);
				buf_idx_ = 0;
			}

			memcpy(block_buf_ + buf_idx_, block_buf, doc_buf_idx * sizeof(int32_t));
			buf_idx_ += doc_buf_idx;
			offset_buf_[j + 1] = offset_buf_[j] + doc_buf_idx;
			total_token += doc_buf_idx / 2;
		}

		if (buf_idx_ != 0)
		{
			block_file.write(reinterpret_cast<char*> (block_buf_), sizeof(int32_t)* buf_idx_);
			buf_idx_ = 0;
		}

		block_file.seekp(4);

		// write the length of each doc in this block
		block_file.write(reinterpret_cast<char*> (offset_buf_), sizeof(int64_t)* (block_size + 1));

		block_file.close();
	}
	LOG(INFO) << "Total tokens: " << total_token;

	input_file.close();
	delete[] block_buf;
}

int main(int argc, char* argv[])
{
	google::ParseCommandLineFlags(&argc, &argv, true);
	google::InitGoogleLogging(argv[0]);

	//buf_size_ = FLAGS_block_size * FLAGS_mean_doc_size;

	int64_t doc_num;
	int32_t block_num;
	std::vector<int32_t> blocks_size;

	// Count the number of documents in input data 
	double count_start = get_time();
	count_doc_num(FLAGS_libsvm_doc, doc_num);
	double count_end = get_time();
	// If FLAGS_block_size is default value as -1, we will make FLAGS_block_size equal to the number of docs
	// in the input file. Otherwise, no change on FLAGS_block_size.
	if (FLAGS_block_size < 0)
	{
		FLAGS_block_size = doc_num;
	}

	double alloc_start = get_time();
	// offset
	offset_buf_ = new int64_t[FLAGS_block_size + 1];
	CHECK(offset_buf_) << "Memory allocation fails: offset_buf";
	block_buf_ = new int32_t[kMaxBufSize];
	CHECK(block_buf_) << "Memory allocation fails: word_buf";

	double alloc_end = get_time();
	LOG(INFO) << "Elapsed seconds for allocation: " << (alloc_end - alloc_start);

	LOG(INFO) << "Total number of docs: " << doc_num;
	LOG(INFO) << "Elapsed seconds for count_doc_num: " << (count_end - count_start);

	// Calculate the number of blocks, the size of each block based on 
	// the total number of documents and the maximum size of each block
	calc_block_num(doc_num, FLAGS_block_size, block_num, blocks_size);

	LOG(INFO) << "Total number of blocks: " << block_num;
	LOG(INFO) << "The size of the last block: " << blocks_size.back();

	// Dump the binary docs into seperate blocks
	double dump_start = get_time();
	dump_blocks(FLAGS_libsvm_doc, FLAGS_binary_doc_dir, block_num, blocks_size);
	double dump_end = get_time();
	LOG(INFO) << "Elapsed seconds for dump blocks: " << (dump_end - dump_start);

	delete[]offset_buf_;
	delete[]block_buf_;
	return 0;
}