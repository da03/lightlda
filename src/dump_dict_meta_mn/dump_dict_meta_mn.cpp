// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date  : 2014.10.05

#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <stdint.h>
#include <limits>
#include <chrono>
#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(word_id_file, "", "word id file created by dump_word_dict.py");
DEFINE_string(dict_meta_file, "", "output file");
DEFINE_int32(load_factor, 2, "load factor of light weight hash table");
DEFINE_int32(num_vocabs, 1000, "the number of vocabs");
DEFINE_int32(num_topics, 1000000, "the number of topics");
DEFINE_int32(num_clients, 8, "the numner of clients");

struct WordEntry
{
	int32_t word_id_;
	int64_t offset_;
	int64_t end_offset_;
	int32_t capacity_;
	int32_t is_dense_;
};

int32_t upper_bound(int32_t x)
{

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
	will be at least |FLAGS_load_factor|           //	for load factor 
	                 * 2                           // for both key and value
					 * |tf|.                       // the word can at most belongs to |tf| distinct topics
    Therefore, if |FLAGS_load_factor| * 2 * |tf| >= |FLAGS_num_topics|, using hash_map will not save any memory space.
	So, if |tf| >= |hot_thresh| = |FLAGS_num_topics| / (2 * |FLAGS_load_factor|), we use dense array to store its word-topic-row,
	if |tf| < |FLAGS_num_topics| / (2 * FLAGS_load_factor), we use light weight hash table to store its word-topic-row.

	We introduce |FLAGS_sparse_factor| to further control what words' topic row will be sparse.
	*/
	//const int32_t hot_thresh = FLAGS_num_topics / (2 * FLAGS_load_factor * FLAGS_sparse_factor);
	const int32_t hot_thresh = FLAGS_num_topics / (2 * FLAGS_load_factor);
	//const int32_t hot_thresh = 0;
	const int32_t max_tf_thresh = std::numeric_limits<int32_t>::max();
	
	// support multi machines
	std::vector<WordEntry* > dict_vector(FLAGS_num_clients);
	std::vector<int32_t> word_count_vector(FLAGS_num_clients, 0);
	std::vector<int64_t> offset_vector(FLAGS_num_clients, 0);
	std::vector<int64_t> tf_in_client(FLAGS_num_clients, 0);

	for (auto& dict : dict_vector) {
		dict = new WordEntry[FLAGS_num_vocabs];
	}
	// WordEntry *dict = new WordEntry[FLAGS_num_vocabs];

	auto start_reading = std::chrono::high_resolution_clock::now();
	std::ifstream word_id_fin(FLAGS_word_id_file, std::ios::in);
	CHECK(word_id_fin.good()) << "Can not open file: " << FLAGS_word_id_file;

	int32_t word_id;
	std::string word;
	int32_t tf;
	// int32_t word_count = 0;
	// int64_t offset = 0;
	
	while (word_id_fin >> word_id >> word >> tf)
	{
		// CHECK(word_count == word_id) << "word_count != word_id";
		CHECK(tf < max_tf_thresh) << "tf of " << word << " is " << tf << " ,exceeding |max_tf_thresh| = " << max_tf_thresh;
		int32_t client_id = word_id % FLAGS_num_clients;
		WordEntry* & dict = dict_vector[client_id];
		int32_t& word_count = word_count_vector[client_id];
		int64_t& offset = offset_vector[client_id];
		int64_t& tf_count = tf_in_client[client_id];
		int32_t table_size = 0;
		int32_t capacity = 0;

		dict[word_count].word_id_ = word_id;
		if (tf >= hot_thresh)
		{
			dict[word_count].is_dense_ = 1;
			table_size = FLAGS_num_topics;
			capacity = table_size;
		}
		else
		{
			dict[word_count].is_dense_ = 0;
			int capacity_lower_bound = FLAGS_load_factor * tf;
			capacity = upper_bound(capacity_lower_bound);
			table_size = capacity * 2;
			// capacity = FLAGS_load_factor * tf;
		}
		dict[word_count].offset_ = offset;
		dict[word_count].end_offset_ = offset + table_size;
		dict[word_count].capacity_ = capacity;

		offset += table_size;
		word_count++;
		tf_count += tf;
	}

	word_id_fin.close();

	//std::cout << "Memory block size: " << offset << std::endl;

	auto end_reading = std::chrono::high_resolution_clock::now();
	auto elapsed = std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(end_reading - start_reading);
	auto elapsed_microseconds = elapsed.count();
	LOG(INFO) << "Elapsed time for reading = " << elapsed_microseconds << " microseconds";

	for (int32_t client_id = 0; client_id < FLAGS_num_clients; ++client_id) {
		std::string client_dict_meta_file = FLAGS_dict_meta_file + "." + std::to_string(client_id);
		std::ofstream dict_meta_fout(client_dict_meta_file, std::ios::out | std::ios::binary);
		CHECK(dict_meta_fout.good()) << "Can not create file: " << client_dict_meta_file;
		LOG(INFO) << "#####################################################";
		LOG(INFO) << "Client id = " << client_id;
		LOG(INFO) << "word count = " << word_count_vector[client_id];
		LOG(INFO) << "tf count = " << tf_in_client[client_id];
		LOG(INFO) << "size of word = " << sizeof(WordEntry)* word_count_vector[client_id];
		LOG(INFO) << "Memory block size (Byte): " << offset_vector[client_id] * 4 << std::endl;
		dict_meta_fout.write(reinterpret_cast<char*>(&word_count_vector[client_id]), sizeof(word_count_vector[client_id]));
		dict_meta_fout.write(reinterpret_cast<char*>(dict_vector[client_id]), sizeof(WordEntry)* word_count_vector[client_id]);
		dict_meta_fout.close();
	}

	for (auto& dict : dict_vector) {
		delete[] dict;
	}
	return 0;
}