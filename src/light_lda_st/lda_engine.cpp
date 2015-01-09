// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2014.03.29

#include "rand_int_rng.h"
#include "lda_engine.hpp"
#include "utils.hpp"
#include "context.hpp"
#include <unordered_map>
#include <cstdint>
#include <string>
#include <cstdlib>
#include <time.h>
#include <glog/logging.h>
#include <mutex>
#include <set>
#include <fstream>

namespace lda {
	LDAEngine::LDAEngine()
	{
		util::Context& context = util::Context::get_instance();
		K_ = context.get_int32("num_topics");
		V_ = context.get_int32("num_vocabs");
		CHECK_NE(-1, V_);	
		
		compute_ll_interval_ = context.get_int32("compute_ll_interval");
	}
	
	void LDAEngine::Start()
	{
		util::Context& context = util::Context::get_instance();
		int32_t num_iterations = context.get_int32("num_iterations");
		std::string output_prefix = context.get_string("output_prefix");
		int dump_iter = context.get_int32("dump_iter");

		double init_start = lda::get_time();
		int token_num = sampler_.GlobalInit(docs_);
		double init_end = lda::get_time(); 
		
		LOG(INFO)
			<< "token num = " << token_num
			<< "\tInit took " << init_end - init_start << " sec"
			<< "\tThroughput: "
			<< static_cast<double>(token_num) / (init_end - init_start) << " (token/sec)";


		LOG(INFO) << "Init sampler OK";

		double total_start = lda::get_time();
		for (int iter = 0; iter <= num_iterations; ++iter)
		{
			num_tokens_clock_ = 0;

			// Sampling.
			sampler_.zero_statistics();
			sampler_.UpdateAlpha(iter);

			double iter_start = lda::get_time();

			for (auto &doc : docs_)
			{
				sampler_.SampleOneDoc(&doc);
				num_tokens_clock_ += doc.token_num();
			}

			double iter_end = lda::get_time();
			sampler_.print_statistics();

			double seconds_this_iter = iter_end - iter_start;
			LOG(INFO)
				<< "Iter: " << iter
				<< "\tTook: " << seconds_this_iter << " sec"
				<< "\tThroughput: "
				<< static_cast<double>(num_tokens_clock_) / seconds_this_iter << " token/(thread*sec)";

			if (compute_ll_interval_ != -1 && iter % compute_ll_interval_ == 0)
			{
				// Every thread compute doc-topic LLH component.
				int ith_llh = iter / compute_ll_interval_;
		
				double doc_ll_start = lda::get_time();

				double doc_ll = 0;
				double word_ll = 0;
				double total_ll = 0;

				int doc_num = 0;
				for (auto &doc : docs_)
				{
					doc_ll += sampler_.ComputeOneDocLLH(&doc);
					doc_num++;
				}

				total_ll += doc_ll;
				LOG(INFO) << "Doc   likelihood: " << doc_ll;

				double doc_ll_end = lda::get_time();

				double word_ll_start = lda::get_time();

				word_ll = sampler_.ComputeWordLLH();
				LOG(INFO) << "Word  likelihood: " << word_ll;

				double word_ll_end = lda::get_time();

				total_ll += word_ll;
				LOG(INFO) << "Total likelihood: " << total_ll;

				LOG(INFO) << "Elapsed seconds for doc ll: " << (doc_ll_end - doc_ll_start);
				LOG(INFO) << "Elapsed seconds for word ll: " << (word_ll_end - word_ll_start);
			}
			if (!output_prefix.empty() && iter && iter % dump_iter == 0)
			{
				Dump(iter);
			}
		}
		double total_end = lda::get_time();
		LOG(INFO) << "Total time for " << num_iterations << " iterations : " << (total_end - total_start) << " sec.";
	}

	void LDAEngine::AddWordTopics(Document *doc, int32_t word, int32_t num_tokens)
	{
		std::vector<int32_t> rand_topics(num_tokens);
		for (int32_t& t : rand_topics)
		{
			t = wood::intel_fast_k(K_);
		}
		doc->add_word_topics(word, rand_topics);
	}
	

	int32_t LDAEngine::ReadData(const std::string& doc_file)
	{
		const char *line = NULL;
		char *endptr = NULL;
		size_t num_bytes;
		int base = 10;
		std::string str_line;
		std::ifstream doc_fp;
		doc_fp.open(doc_file.c_str(), std::ios::in);
		LOG(INFO) << "Open file";
		CHECK(doc_fp.good()) << "Failed to open file " << doc_file;

		docs_.clear();
		int num_docs = 0;
		while (getline(doc_fp, str_line))
		{
			str_line += "\n";
			line = str_line.c_str();
			Document doc;
			strtol(line, &endptr, base); // ignore first field (category label)
			char *ptr = endptr;
			while (*ptr == '\t' || *ptr == ' ') ++ptr; // goto next non-space char
			while (*ptr != '\n')
			{
				// read a word_id:count pair
				int32_t word_id = strtol(ptr, &endptr, base);
				ptr = endptr; // *ptr = colon
				CHECK_EQ(':', *ptr) << "num_docs = " << num_docs;
				int32_t count = strtol(++ptr, &endptr, base);
				ptr = endptr;

				AddWordTopics(&doc, word_id, count);
				
				local_vocabs_.insert(word_id);
				while (*ptr == ' ') ++ptr; // goto next non-space char
			}
			if (doc.token_num())
			{
				docs_.push_back(std::move(doc));
				++num_docs;
			}
		}
		doc_fp.close();
		LOG(INFO) << "Load doc OK";

		int32_t docs_per_thread = static_cast<int>(num_docs);
		LOG(INFO) << "Read " << num_docs << " documents which uses "
			<< local_vocabs_.size()
			<< " local_vocabs_"
			<< " threads. Each thread roughly has "
			<< docs_per_thread << " docs";
		
		return local_vocabs_.size();
	}

	void LDAEngine::Dump(int iter)
	{
		util::Context& context = util::Context::get_instance();

		int num_topics = context.get_int32("num_topics");
		std::string output_prefix = context.get_string("output_prefix");
		std::string doc_file = context.get_string("doc_file");
		int32_t num_vocabs = context.get_int32("num_vocabs");
		int32_t num_iterations = context.get_int32("num_iterations");
		double alpha_sum = context.get_double("alpha_sum");
		double beta = context.get_double("beta");
		int32_t mh_step = context.get_double("mh_step");
		int32_t gs_type = context.get_int32("gs_type");

		// dump word-topic counts
		std::string word_topic_dump = doc_file +
			".word_topic.alpha_sum" + std::to_string(alpha_sum) + ".beta."
			+ std::to_string(beta) + ".mh_step." + std::to_string(mh_step) +
			+".gs_type." + std::to_string(gs_type) +
			".iter." + std::to_string(iter) + ".vocab." +
			std::to_string(num_vocabs) + ".topic." + std::to_string(num_topics) + "." + output_prefix;


		LOG(INFO) << "Writing output to " << word_topic_dump << " ...";

		sampler_.Dump(word_topic_dump);


		LOG(INFO) << "Dump completes";	
	}

}   // namespace lda
