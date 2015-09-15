// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date: 2014.09.14

#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <stdint.h>
#include <chrono>
#include <algorithm>
#include <set>
#include <map>
#include <tuple>

#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/progress.hpp>
#include <boost/tokenizer.hpp>
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
11   // the offset of the 2-nd doc, with this we know the length of the 1-st doc is 5 = 10/2
18   // the offset of the 3-rd doc, with this we know the length of the 2-nd doc is 3 = (16-10)/2 
27   // with this, we know the length of the 3-rd doc is 4 = (24 - 16)/2
idx w11 t11 w12 t12 w13 t13 w14 t14 w15 t15     // the token-topic list of the 1-st doc
idx w21 t21 w22 t22 w23 t23                     // the token-topic list of the 2-nd doc
idx w31 t31 w32 t32 w33 t33 w34 t34             // the token-topic list of the 3-rd doc
*/

struct Token {
	int32_t word_id;
	int32_t topic_id;
};

int Compare(const Token& token1, const Token& token2) {
	return token1.word_id < token2.word_id;
}

DEFINE_int64(block_size, 1000000, "the maximum number of docs in each block");
DEFINE_int64(mean_doc_size, 100, "the average number of tokens in each doc");
DEFINE_int32(file_offset, 0, "The offset of the output file name");
DEFINE_string(datablocks_dir, "", "");
DEFINE_string(input_dir, "", "");
DEFINE_string(vocab_stopword, "", "");
DEFINE_int32(vocab_min_occurence, 0, "");

int64_t buf_idx_;
int64_t buf_size_;

int64_t *offset_buf_;      // size: FLAGS_block_size + 1
int32_t *block_buf_;       // size: buf_size

const int kMaxVocabSize = 50000000;
const int kMaxTokenNum = 2000000;
// std::map<int32_t, int32_t> global_tf_map;
std::vector<int32_t> global_tf_map(kMaxVocabSize, 0);


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
		if (doc_num % 100000 == 0)
		{
			std::cout << "Processed line #: " << doc_num << std::endl;
		}
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

void dump_blocks(std::vector<std::string> &filenames, std::map<std::string, int32_t> &word_to_id,
        std::string output_dir, int32_t block_num, std::vector<int32_t> &blocks_size)
{
	const char* line = NULL;
	char *endptr = NULL;
	const int kBASE = 10;

	int64_t total_token = 0;

	int doc_buf_idx;
	int32_t *block_buf;

	block_buf = new int32_t[kMaxTokenNum];
	int32_t cursor = 0;
    int32_t doc_idx = 0;
	for (int i = 0; i < block_num; ++i)
	{
		std::cout << "Start dumping block# " << i + FLAGS_file_offset << std::endl;

		std::string block_name = output_dir + "/block." + std::to_string(i + FLAGS_file_offset);
		std::string vocab_name = output_dir + "/vocab." + std::to_string(i + FLAGS_file_offset);
		std::string txt_vocab_name = output_dir + "/vocab." + std::to_string(i + FLAGS_file_offset) + ".txt";
		
		std::ofstream block_file(block_name, std::ios::out | std::ios::binary);
		std::ofstream vocab_file(vocab_name, std::ios::out | std::ios::binary);
		std::ofstream txt_vocab_file(txt_vocab_name, std::ios::out);

		CHECK(block_file.good()) << "Fails to create file: " << block_name;
		CHECK(vocab_file.good()) << "Fails to create file: " << vocab_name;
		CHECK(txt_vocab_file.good()) << "Fails to create file: " << txt_vocab_name;

		// std::map<int32_t, int32_t> local_vocabs;
		std::vector<int32_t> local_vocabs(kMaxVocabSize, 0);
		int block_size = blocks_size[i];

		std::string str_line;

		memset(offset_buf_, 0, sizeof(int64_t)* (block_size + 1));

		// write the number of docs in this block
		block_file.write(reinterpret_cast<char*> (&block_size), sizeof(int32_t));

		// write the length of each doc in this block
		block_file.write(reinterpret_cast<char*> (offset_buf_), sizeof(int64_t)* (block_size + 1));

		int64_t block_token_num = 0;

		buf_idx_ = 0;
		for (int j = 0; j < block_size; ++j)
		{			
			int doc_count = 0;
			std::vector<Token> doc_tokens;
            std::string filename = filenames[doc_idx++];
	        std::ifstream input_file(filename, std::ios::in);
	        CHECK(input_file.good()) << "Fails to open file: " << filename;
            std::string str_line;
            bool exceed_flag = false;
            while (getline(input_file, str_line) && !exceed_flag) {
		        if (!str_line.empty()) {
                    boost::tokenizer<> tok(str_line);
                    for (boost::tokenizer<>::iterator itr=tok.begin(); itr!=tok.end(); ++itr) {
                        std::string token = *itr;
                        std::transform(token.begin(), token.end(), token.begin(), ::tolower);
                        std::map<std::string, int32_t>::iterator it = word_to_id.find(token);
                        if (it != word_to_id.end()) {
                            int32_t word_id = it->second;
				            doc_tokens.push_back({ word_id, 0 });
				            ++local_vocabs[word_id];
				            ++block_token_num;
				            ++doc_count;
				            if (doc_count >= 512) {
                                exceed_flag = true;
                                break;
                            }
                        }
                    }
                }
            }
            input_file.close();

			doc_buf_idx = 0;

			std::sort(doc_tokens.begin(), doc_tokens.end(), Compare);

			block_buf[doc_buf_idx++] = 0; // cursor
			
			for (auto& token : doc_tokens) 
			{
				block_buf[doc_buf_idx++] = token.word_id;
				block_buf[doc_buf_idx++] = token.topic_id;
			}

			if (buf_idx_ + doc_buf_idx > buf_size_)
			{
				block_file.write(reinterpret_cast<char*> (block_buf_), sizeof(int32_t)* buf_idx_);
				buf_idx_ = 0;
			}

			memcpy(block_buf_ + buf_idx_, block_buf, doc_buf_idx * sizeof(int32_t));
			buf_idx_ += doc_buf_idx;
			offset_buf_[j + 1] = offset_buf_[j] + doc_buf_idx;
			total_token += doc_buf_idx / 2;
		}

		// add local vocabs end offset.
		// offset_buf_[block_size + 1] = offset_buf_[block_size] + local_vocabs.size();

		if (buf_idx_ != 0)
		{
			block_file.write(reinterpret_cast<char*> (block_buf_), sizeof(int32_t)* buf_idx_);
			buf_idx_ = 0;
		}


		block_file.seekp(4);

		// write the length of each doc in this block
		block_file.write(reinterpret_cast<char*> (offset_buf_), sizeof(int64_t)* (block_size + 1));
		block_file.close();

		int32_t vocab_size = 0;
		
		vocab_file.write(reinterpret_cast<char*>(&vocab_size), sizeof(int32_t));
		int32_t non_zero_count = 0;
		// write vocab
		for (int i = 0; i < local_vocabs.size(); ++i) 
		{
			if (local_vocabs[i] > 0)
			{
				non_zero_count++;
				vocab_file.write(reinterpret_cast<char*> (&i), sizeof(int32_t));
			}
		}
		LOG(INFO) << "The number of tokens in block " << i << " is: " << block_token_num;
		LOG(INFO) << "Local vocab_size for block " << i << " is: " << non_zero_count;

		std::cout << "The number of tokens in block " << i << " is: " << block_token_num << std::endl;
		std::cout << "Local vocab_size for block " << i << " is: " << non_zero_count << std::endl;
		
		//std::ofstream txt_vocab

		// write global tf
		for (int i = 0; i < local_vocabs.size(); ++i)
		{
			if (local_vocabs[i] > 0)
			{
				vocab_file.write(reinterpret_cast<char*> (&global_tf_map[i]), sizeof(int32_t));
			}
		}
		// write local tf
		for (int i = 0; i < local_vocabs.size(); ++i)
		{
			if (local_vocabs[i] > 0)
			{
				vocab_file.write(reinterpret_cast<char*> (&local_vocabs[i]), sizeof(int32_t));
			}
		}
		vocab_file.seekp(0);
		vocab_file.write(reinterpret_cast<char*>(&non_zero_count), sizeof(int32_t));

		vocab_file.close();

		txt_vocab_file << non_zero_count << std::endl;
		for (int i = 0; i < local_vocabs.size(); ++i)
		{
			if (local_vocabs[i] > 0)
			{
				// vocab_file.write(reinterpret_cast<char*> (&local_vocabs[i]), sizeof(int32_t));
				txt_vocab_file << i << "\t" << global_tf_map[i] << "\t" << local_vocabs[i] << std::endl;
			}
		}
		txt_vocab_file.close();
	}
	LOG(INFO) << "Total tokens: " << total_token;
	std::cout << "Total tokens: " << total_token << std::endl;

	delete[] block_buf;
}

void get_filenames(std::string input_dir, std::vector<std::string> &filenames) {

    boost::filesystem::directory_iterator end_itr;
    LOG(INFO) << input_dir;
    for( boost::filesystem::directory_iterator itr(input_dir); itr != end_itr; ++itr)
    {
        if(!boost::filesystem::is_regular_file(itr->status())) continue;
        std::string filename = itr->path().string(); 
        filenames.push_back(filename);
    }
}

void count_tf_df(std::vector<std::string> &filenames, 
        std::map<std::string, std::pair<int32_t, int32_t> > &global_tf_df) {
    for (std::string filename: filenames) {
	    std::ifstream input_file(filename, std::ios::in);
	    CHECK(input_file.good()) << "Fails to open file: " << filename;
        std::string str_line;
        std::map<std::string, int32_t> document_df;
        while (getline(input_file, str_line)) {
		    if (!str_line.empty()) {
                boost::tokenizer<> tok(str_line);
                for (boost::tokenizer<>::iterator itr=tok.begin(); itr!=tok.end(); ++itr) {
                    std::string token = *itr;
                    std::transform(token.begin(), token.end(), token.begin(), ::tolower);
                    if (global_tf_df.find(token) == global_tf_df.end()) {
                        global_tf_df[token] = std::make_pair<int32_t, int32_t>(0,0);
                    }
                    int tf = global_tf_df[token].first;
                    int df = global_tf_df[token].second;
                    ++tf;
                    if (document_df.find(token) == document_df.end()) {
                        ++df;
                        document_df[token] = 1;
                    }
                    global_tf_df[token].first = tf;
                    global_tf_df[token].second = df;
                }
            }
        }
        input_file.close();
    }
}

void remove_stopwords(std::string stopword_filename,
        std::map<std::string, std::pair<int32_t, int32_t> > &global_tf_df) {
	std::ifstream stopword_file(stopword_filename, std::ios::in);
	CHECK(stopword_file.good()) << "Fails to open file: " << stopword_filename;
    std::string stopword;
    while (stopword_file >> stopword) {
        std::map<std::string, std::pair<int32_t, int32_t> >::iterator it = global_tf_df.find(stopword);
        if (it != global_tf_df.end()) {
            global_tf_df.erase(it);
        }
    }
    stopword_file.close();
}

void dump_word_dict(std::vector<std::tuple<std::string, int32_t, int32_t> > &tf_df_vec, std::string datablocks_dir) {
    std::string word_dict_filename = datablocks_dir + "/word_tf.txt";
    std::ofstream word_dict_file(word_dict_filename, std::ios::out);
    CHECK(word_dict_file.good()) << "Fails to create file: " << word_dict_filename;
    int id = 0;
    for (std::tuple<std::string, int32_t, int32_t> tf_df : tf_df_vec) {
        word_dict_file << id++ << " " << std::get<0>(tf_df) << " " << std::get<1>(tf_df) << std::endl;
    }
    word_dict_file.close();
}
int main(int argc, char* argv[]) {
	google::ParseCommandLineFlags(&argc, &argv, true);
	google::InitGoogleLogging(argv[0]);

    // LOG(INFO) << "Process part# " << FLAGS_file_offset;

    std::srand(unsigned(std::time(0)));

    std::vector<std::string> filenames;

    get_filenames(FLAGS_input_dir, filenames);

    std::map<std::string, std::pair<int32_t, int32_t> > global_tf_df;
    count_tf_df(filenames, global_tf_df);
    remove_stopwords(FLAGS_vocab_stopword, global_tf_df);
    std::vector<std::tuple<std::string, int32_t, int32_t> > tf_df_vec;
    for (std::map<std::string, std::pair<int32_t, int32_t> >::iterator it = global_tf_df.begin();
            it != global_tf_df.end(); ++it) {
        tf_df_vec.push_back(std::make_tuple(it->first, it->second.first, it->second.second));
    }
    std::sort(tf_df_vec.begin(), tf_df_vec.end(), [](std::tuple<std::string, int32_t, int32_t> item1,
                std::tuple<std::string, int32_t, int32_t> item2){return std::get<1>(item1) > std::get<1>(item2);});
    std::vector<std::tuple<std::string, int32_t, int32_t> >::iterator itr = 
        std::lower_bound(tf_df_vec.begin(), tf_df_vec.end(), 
                std::make_tuple("", FLAGS_vocab_min_occurence-1, 0), [](std::tuple<std::string, int32_t, int32_t> item1, 
                    std::tuple<std::string, int32_t, int32_t> item2){return std::get<1>(item1) > std::get<1>(item2);});
    tf_df_vec.erase(itr, tf_df_vec.end());
    std::random_shuffle(tf_df_vec.begin(), tf_df_vec.end());
    dump_word_dict(tf_df_vec, FLAGS_datablocks_dir);
    std::map<std::string, int32_t> word_to_id;
    int id = 0;
    for (auto tf_df : tf_df_vec) {
        global_tf_map[id] = std::get<1>(tf_df);
        word_to_id[std::get<0>(tf_df)] = id++;
    }
	buf_size_ = 10000 * FLAGS_mean_doc_size;

	std::cout << "FLAGS_block_size = " << FLAGS_block_size << std::endl;
	std::cout << "FLAGS_mean_doc_size = " << FLAGS_mean_doc_size << std::endl;
	std::cout << "buf_size_ = " << buf_size_ << std::endl;

	int64_t doc_num = filenames.size();
	int32_t block_num;
	std::vector<int32_t> blocks_size;

	double alloc_start = get_time();
	// offset
	offset_buf_ = new int64_t[FLAGS_block_size + 2];
	CHECK(offset_buf_) << "Memory allocation fails: offset_buf";

	//std::cout << "offset_buf_ malloc ok" << std::endl;

	block_buf_ = new int32_t[buf_size_];
	CHECK(block_buf_) << "Memory allocation fails: word_buf";

	std::cout << "block_buf_ malloc ok" << std::endl;

	double alloc_end = get_time();
	LOG(INFO) << "Elapsed seconds for allocation: " << (alloc_end - alloc_start);

	LOG(INFO) << "Total number of docs: " << doc_num;

	std::cout << "Calculating how many blocks to split..." << std::endl;

	// Calculate the number of blocks, the size of each block based on 
	// the total number of documents and the maximum size of each block
	calc_block_num(doc_num, FLAGS_block_size, block_num, blocks_size);

	LOG(INFO) << "Total number of blocks: " << block_num;
	LOG(INFO) << "The size of general blocks: " << blocks_size.front();
	LOG(INFO) << "The size of the last block: " << blocks_size.back();
	std::cout << "Total number of blocks: " << block_num << std::endl;
	std::cout << "The size of general blocks: " << blocks_size.front() << std::endl;
	std::cout << "The size of the last block: " << blocks_size.back() << std::endl;
	

	std::cout << "Dump binary blocks..." << std::endl;

	// Dump the binary docs into seperate blocks
	double dump_start = get_time();
	dump_blocks(filenames, word_to_id, FLAGS_datablocks_dir, block_num, blocks_size);
	double dump_end = get_time();
	LOG(INFO) << "Elapsed seconds for dump blocks: " << (dump_end - dump_start);

	delete[]offset_buf_;
	delete[]block_buf_;

	std::cout << "Success!" << std::endl;
	return 0;
}
