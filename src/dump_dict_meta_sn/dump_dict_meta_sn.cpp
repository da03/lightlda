// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date  : 2014.10.05

#include <string>
#include <iostream>
#include <fstream>
#include <stdint.h>
#include <limits>
#include <chrono>
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(word_id_file, "", "word id file created by dump_word_dict.py");
DEFINE_string(dict_meta_file, "", "output file");
DEFINE_int32(num_vocabs, 1000000, "the number of vocabs");
DEFINE_int32(num_topics, 1000, "the number of topics");

DEFINE_int32(load_factor, 2, "load factor of light weight hash table");
DEFINE_int32(sparse_factor, 5, "control the sparsity of hash table");


struct WordEntry
{
	int32_t word_id_;
	int64_t offset_;            
	int64_t end_offset_;
	int32_t capacity_;
	int32_t is_dense_;

	int32_t tf;
	int64_t alias_offset_;
	int64_t alias_end_offset_;
	int32_t alias_capacity_;
	int32_t is_alias_dense_;
};

int32_t upper_bound(int32_t x)
{
	if (x == 0)
	{
		return 0;
	}
	int32_t shift = 0;
	int32_t y = 1;
	x--;
	while (x)
	{
		x = x >> 1;
		y = y << 1;
		++shift;
	}
	return y;
}

int32_t align64(int32_t size)
{
	if (size % 64 == 0)
	{
		return size;
	}
	else
	{
		size = 64 * (size / 64) + 64;
		return size;
	}
}

int main(int argc, char* argv[])
{
	google::ParseCommandLineFlags(&argc, &argv, true);
	google::InitGoogleLogging(argv[0]);

	/*
	int32_t v = 10000;
	int32_t k = 1000000;

	int32_t size32 = k * v;
	int64_t size64 = k * v;
	int64_t size64_2 = (int64_t)k * v;
	*/

	/*
	If the |tf| of a word exceeds |hot_thresh|, the memory footprint of its hash table
	will be at least |FLAGS_load_factor|           // for load factor 
	                 * 2                           // for both key and value
					 * |tf|.                       // the word can at most belongs to |tf| distinct topics
    Therefore, if |FLAGS_load_factor| * 2 * |tf| >= |FLAGS_num_topics|, using hash_map will not save any memory space.
	So, if |tf| >= |hot_thresh| = |FLAGS_num_topics| / (2 * |FLAGS_load_factor|), we use dense array to store its word-topic-row,
	if |tf| < |FLAGS_num_topics| / (2 * FLAGS_load_factor), we use light weight hash table to store its word-topic-row.

	We introduce |FLAGS_sparse_factor| to further control what words' topic row will be sparse.
	*/
	const int32_t max_tf_thresh = std::numeric_limits<int32_t>::max();
	const int32_t hot_thresh = FLAGS_num_topics / (2 * FLAGS_load_factor);  //hybrid
	//const int32_t hot_thresh = 0;  // totally dense
	//const int32_t hot_thresh = max_tf_thresh;  // totally sparse
	const int32_t alias_hot_thresh = (FLAGS_num_topics * 2) / 3;

	//const int32_t hot_thresh = 0;

	WordEntry *dict = new WordEntry[FLAGS_num_vocabs];

	auto start_reading = std::chrono::high_resolution_clock::now();
	std::ifstream word_id_fin(FLAGS_word_id_file, std::ios::in);
	CHECK(word_id_fin.good()) << "Can not open file: " << FLAGS_word_id_file;

	int32_t word_id;
	std::string word;
	int32_t tf;
	int32_t word_count = 0;
	int64_t offset = 0;

	int32_t table_size = 0;
	int32_t capacity = 0;
	
	int64_t element_size = 0;
	int32_t dense_num = 0;
	int64_t global_tf = 0;
	int64_t dense_tf = 0;

	int64_t alias_offset = 0;
	word = "null";
	while (word_id_fin >> word_id >> word >> tf)
	//while (word_id_fin >> word_id >> tf)
	{
		global_tf += tf;

		CHECK(word_count == word_id) << "word_count != word_id";
		CHECK(tf < max_tf_thresh) << "tf of " << word << " is " << tf << " ,exceeding |max_tf_thresh| = " << max_tf_thresh;

		dict[word_count].word_id_ = word_id;
		dict[word_count].tf = tf;

		if (tf >= hot_thresh)
		{
			dict[word_count].is_dense_ = 1;
			table_size = FLAGS_num_topics;
			capacity = table_size;
			element_size += table_size;

			dense_num++;
			dense_tf += tf;
		}
		else
		{
			dict[word_count].is_dense_ = 0;
			int capacity_lower_bound = FLAGS_load_factor * tf;
			capacity = upper_bound(capacity_lower_bound);
			table_size = capacity * 2;
			element_size += capacity_lower_bound * 2;
		}

		dict[word_count].offset_ = offset;
		dict[word_count].end_offset_ = offset + table_size;
		dict[word_count].capacity_ = capacity;

		offset += table_size;

		if (tf >= alias_hot_thresh)
		{
			//int32_t buf_size = align64(2 * FLAGS_num_topics);
			int32_t buf_size = 2 * FLAGS_num_topics;

			dict[word_count].alias_capacity_ = FLAGS_num_topics;
			dict[word_count].alias_offset_ = alias_offset;
			dict[word_count].alias_end_offset_ = alias_offset + buf_size; // FLAGS_num_topics * (k, v)
			dict[word_count].is_alias_dense_ = 1;
			alias_offset += buf_size;

		}
		else
		{
			//int32_t buf_size = align64(3 * tf);
			int32_t buf_size = 3 * tf;

			dict[word_count].alias_capacity_ = tf;
			dict[word_count].alias_offset_ = alias_offset;
			dict[word_count].alias_end_offset_ = alias_offset + buf_size;              // tf * (k,v,idx) 
			dict[word_count].is_alias_dense_ = 0;
			alias_offset += buf_size;
		}


		word_count++;
		if (word_count % 10 == 0)
		{
			//std::cout << word_count << std::endl;
		}
	}

	word_id_fin.close();

	int32_t entry_size = sizeof(WordEntry);

	std::cout << "number of dense rows: " << dense_num << std::endl;
	std::cout << "global_tf = " << global_tf << std::endl;
	std::cout << "dense_tf = " << dense_tf << std::endl;

	std::cout << "word_topic_table:" << std::endl;
	std::cout << "Element size: " << element_size * 4 << std::endl;
	std::cout << "Memory block size: " << offset * 4 << " Bytes" << std::endl;

	std::cout << "alias table: " << std::endl;
	std::cout << "Memory block for alias table: " << alias_offset * 4 << " Bytes" << std::endl;

	auto end_reading = std::chrono::high_resolution_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(end_reading - start_reading);
	auto elapsed_microseconds = elapsed.count();
	LOG(INFO) << "Elapsed time for reading = " << elapsed_microseconds << " microseconds";


	std::ofstream dict_meta_fout(FLAGS_dict_meta_file, std::ios::out | std::ios::binary);
	CHECK(dict_meta_fout.good()) << "Can not create file: " << FLAGS_dict_meta_file;


	dict_meta_fout.write(reinterpret_cast<char*>(&word_count), sizeof(word_count));
	dict_meta_fout.write(reinterpret_cast<char*>(dict), sizeof(WordEntry) * FLAGS_num_vocabs);	

	dict_meta_fout.close();

	delete[]dict;
	return 0;
}