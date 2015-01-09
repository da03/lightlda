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
#include <thread>
#include <Windows.h>

namespace lda {
	LDAEngine::LDAEngine()
	{
		util::Context& context = util::Context::get_instance();
		K_ = context.get_int32("num_topics");
		V_ = context.get_int32("num_vocabs");
		CHECK_NE(-1, V_);	
		
		compute_ll_interval_ = context.get_int32("compute_ll_interval");

		int32_t num_threads = context.get_int32("num_threads");
		compute_ll_interval_ = context.get_int32("compute_ll_interval");
		slice_num_ = context.get_int32("num_slices");
		std::string doc_file = context.get_string("doc_file");
		std::string meta_name = context.get_string("meta_name");

		process_barrier_.reset(new boost::barrier(num_threads));

		double alloc_start = lda::get_time();

		model_block_.Read(meta_name);
		global_word_topic_table_.resize(V_);		
		/*
		for (int i = 0; i < V_; ++i)
		{
			global_word_topic_table_[i] = model_block_.get_row(i);
		}
		*/

		beta_ = context.get_double("beta");
		beta_sum_ = beta_ * V_;

		alias_rng_int_.Init(K_);
		beta_k_v_.resize(K_);

		global_alias_k_v_.resize(V_);
		
		for (int i = 0; i < V_; ++i)
		{
			global_alias_k_v_[i] = model_block_.get_alias_row(i);
		}
		


		global_summary_row_.resize(K_);

		double alloc_end = lda::get_time();
		std::cout << "Elpased secs for allocation:  " << (alloc_end - alloc_start) << std::endl;

		word_range_for_each_thread_.resize(num_threads + 1);
		int32_t word_num_each_thread = V_ / num_threads;
		word_range_for_each_thread_[0] = 0;
		for (int32_t i = 0; i < num_threads - 1; ++i)
		{
			word_range_for_each_thread_[i + 1] = word_range_for_each_thread_[i] + word_num_each_thread;
		}
		word_range_for_each_thread_[num_threads] = V_;

		slice_range_.resize(slice_num_);
		int32_t slice_size = V_ / slice_num_;
		for (int32_t i = 0; i < slice_num_ - 1; ++i)
		{
			slice_range_[i] = slice_size;
		}
		slice_range_[slice_num_ - 1] = V_;

		data_block_.reset(new lda::LDADataBlock(doc_file, num_threads));

		samplers_ = new LightDocSampler*[num_threads];
		for (int i = 0; i < num_threads; ++i)
		{
			samplers_[i] = new LightDocSampler(global_word_topic_table_,
				global_summary_row_,
				global_alias_k_v_,
				beta_height_,
				beta_mass_,
				beta_k_v_);
		}		
	}
	
	void LDAEngine::Start()
	{
		int thread_id = thread_counter_++;

		util::Context& context = util::Context::get_instance();
		int32_t num_iterations = context.get_int32("num_iterations");
		std::string output_prefix = context.get_string("output_prefix");
		int dump_iter = context.get_int32("dump_iter");
		int num_threads = context.get_int32("num_threads");

		long long maskLL = 0;
		maskLL |= (1LL << (thread_id));
		DWORD_PTR mask = maskLL;
		SetThreadAffinityMask(GetCurrentThread(), mask);
		
		process_barrier_->wait();

		LightDocSampler &sampler_ = *(samplers_[thread_id]);

		sampler_.build_word_topic_table(thread_id, num_threads, model_block_);

		process_barrier_->wait();

		double init_start = lda::get_time();

		int32_t token_num = 0;
		int32_t doc_id = 0;
		int32_t doc_start = data_block_->Begin(thread_id);
		int32_t doc_end = data_block_->End(thread_id);

		for (int32_t doc_index = doc_start; doc_index != doc_end; ++doc_index)
		{
			std::shared_ptr<LDADocument> doc = data_block_->GetOneDoc(doc_index);

			int doc_size = doc->size();
			for (int i = 0; i < doc_size; ++i)
			{
				doc->SetTopic(i, sampler_.rand_k());
			}
			token_num += sampler_.GlobalInit(doc.get());
		}
		
		double init_end = lda::get_time();

		if (thread_id == 0)
		{
			std::cout << "Global Init OK, thread = " << thread_id << std::endl;
		}

		LOG(INFO)
			<< "Thread ID = " << thread_id
			<< "\ttoken num = " << token_num
			<< "\tInit took " << init_end - init_start << " sec"
			<< "\tThroughput: "
			<< static_cast<double>(token_num) / (init_end - init_start) << " (token/sec)";

		if (thread_id == 0)
		{
			std::cout << "Start aggreating word_topic_delta, thread = " << thread_id << std::endl;
		}

		double global_start = lda::get_time();

		process_barrier_->wait();

		for (int i = 0; i < num_threads; ++i)
		{
			std::vector<word_topic_delta> & wtd_vec = samplers_[i]->get_word_topic_delta(thread_id);
			for (auto& wtd : wtd_vec)
			{
				global_word_topic_table_[wtd.word].inc(wtd.topic, wtd.delta);
			}
		}


		process_barrier_->wait();
		
		double global_end = lda::get_time();

		if (thread_id == 0)
		{
			std::cout << "End aggregating word_topic_delta, thread = " << thread_id << ", elpased time = " << (global_end - global_start) << std::endl;
		}

		process_barrier_->wait();

		// use thread-private delta table to get global table
		{
			std::lock_guard<std::mutex> lock(global_mutex_);

			std::vector<int64_t> &summary = sampler_.get_delta_summary_row();
			for (int i = 0; i < K_; ++i)
			{
				global_summary_row_[i] += summary[i];
			}
		}
		process_barrier_->wait();
		double summary_end = lda::get_time();

		if (thread_id == 0)
		{
			std::cout << "End aggregating summary_delta, thread = " << thread_id << ", elpased time = " << (summary_end - global_end) <<  std::endl;
		}

		double total_start = lda::get_time();
		for (int iter = 0; iter <= num_iterations; ++iter)
		{
			int32_t token_sweeped = 0;
			num_tokens_clock_ = 0;

			// build alias table

			// 1, build alias table for the dense term,  beta_k_v_, which is shared by all the words
			if (thread_id == 0)
			{
				beta_mass_ = 0;
				std::vector<float> proportion(K_);
				for (int k = 0; k < K_; ++k)
				{
					proportion[k] = beta_ / (global_summary_row_[k] + beta_sum_);
					beta_mass_ += proportion[k];
				}
				alias_rng_int_.SetProportionMass(proportion, beta_mass_, beta_k_v_, &beta_height_);

				std::cout << "Start build alias table" << std::endl;
			}

			double alias_start = lda::get_time();

			// 2,  build alias table for the sparse term
			process_barrier_->wait();


			sampler_.build_alias_table(word_range_for_each_thread_[thread_id], word_range_for_each_thread_[thread_id + 1]);

			process_barrier_->wait();	

			double alias_end = lda::get_time();

			if (thread_id == 0)
			{
				LOG(INFO) << "Elapsed time for building alias table: " << (alias_end - alias_start);
				std::cout << "Elapsed time for building alias table: " << (alias_end - alias_start) << std::endl;
			}




			sampler_.EpocInit();

			process_barrier_->wait();

			//continue;

			// Sampling.
			sampler_.zero_statistics();

			double iter_start = lda::get_time();

			if (thread_id == 0)
			{
				std::cout << "start sampling, thread = " << thread_id << std::endl;
			}

			for (int slice_idx = 0; slice_idx < slice_num_; ++slice_idx)
			{
				int slice_upper = slice_range_[slice_idx];

				int32_t doc_id = 0;
				int32_t doc_start = data_block_->Begin(thread_id);
				int32_t doc_end = data_block_->End(thread_id);

				for (int32_t doc_index = doc_start; doc_index != doc_end; ++doc_index)
				{
					std::shared_ptr<LDADocument> doc = data_block_->GetOneDoc(doc_index);
					num_tokens_clock_ += sampler_.SampleOneDoc(doc.get(), slice_upper);
				}
			}

			double iter_end = lda::get_time();

			if (thread_id == 0)
			{
				double seconds_this_iter = iter_end - iter_start;

				std::cout << "end sampling, thread = " << thread_id << ", elpased time = " << (seconds_this_iter) << std::endl;


				LOG(INFO)
					<< "\tIter: " << iter
					<< "\tThread = " << thread_id
					<< "\tTook: " << seconds_this_iter << " sec"
					<< "\tThroughput: "
					<< static_cast<double>(num_tokens_clock_) / (seconds_this_iter) << " token/(thread*sec)";

			}

			double sync_start = lda::get_time();

			process_barrier_->wait();

			for (int i = 0; i < num_threads; ++i)
			{
				std::vector<word_topic_delta> & wtd_vec = samplers_[i]->get_word_topic_delta(thread_id);
				for (auto& wtd : wtd_vec)
				{
					global_word_topic_table_[wtd.word].inc(wtd.topic, wtd.delta);
				}
			}

			// use thread-private delta table to update global table
			{
				std::lock_guard<std::mutex> lock(global_mutex_);				
	
				std::vector<int64_t> &summary = sampler_.get_delta_summary_row();
				for (int i = 0; i < K_; ++i)
				{
					global_summary_row_[i] += summary[i];
				}
			}

			process_barrier_->wait();

			double sync_end = lda::get_time();

			if (compute_ll_interval_ != -1 && iter % compute_ll_interval_ == 0)
			{
				double doc_ll = 0;
				double word_ll = 0;

				if (thread_id == 0)
				{
					doc_ll_ = 0;
					word_ll_ = 0;

					LOG(INFO) << "Start ll, thread = " << thread_id;
					std::cout << "start ll, thread = " << thread_id << std::endl;
				}

				double doc_ll_start = lda::get_time();

				process_barrier_->wait();

				int doc_num = 0;
				int32_t doc_start = data_block_->Begin(thread_id);
				int32_t doc_end = data_block_->End(thread_id);


				for (int32_t doc_index = doc_start; doc_index != doc_end; ++doc_index)
				{
					if (doc_num > 1000)
					{
						break;
					}

					std::shared_ptr<LDADocument> doc = data_block_->GetOneDoc(doc_index);
					doc_ll += sampler_.ComputeOneDocLLH(doc.get());
					doc_num++;
				}
				doc_ll_ = doc_ll_ + doc_ll;

				process_barrier_->wait();
				double doc_ll_end = lda::get_time();
				if (thread_id == 0)
				{
					LOG(INFO) << "End doc ll, thread = " << thread_id << ", elpased time = " << (doc_ll_end - doc_ll_start);
					std::cout << "End doc ll, thread = " << thread_id << ", elpased time = " << (doc_ll_end - doc_ll_start) << std::endl;
				}

				double word_ll_start = lda::get_time();

				process_barrier_->wait();

				word_ll = sampler_.ComputeWordLLH(word_range_for_each_thread_[thread_id], word_range_for_each_thread_[thread_id + 1]);
				word_ll_ = word_ll_ + word_ll;

				process_barrier_->wait();

				double word_ll_end = lda::get_time();
				if (thread_id == 0)
				{
					LOG(INFO) << "End word ll, thread = " << thread_id << ", elpased time = " << (word_ll_end - word_ll_start);
					std::cout << "End word ll, thread = " << thread_id << ", elpased time = " << (word_ll_end - word_ll_start) << std::endl;
				}

				process_barrier_->wait();

				if (thread_id == 0)
				{
					word_ll_ = word_ll_ + sampler_.NormalizeWordLLH();

					double total_ll = 0;
					LOG(INFO) << "Doc   likelihood: " << doc_ll_;
					total_ll += doc_ll_;
					LOG(INFO) << "Word  likelihood: " << word_ll_;
					total_ll += word_ll_;
					LOG(INFO) << "Total likelihood: " << total_ll;

					LOG(INFO) << "End ll, thread = " << thread_id << ", elpased time = " << (word_ll_end - doc_ll_start);
					std::cout << "End ll, thread = " << thread_id << ", elpased time = " << (word_ll_end - doc_ll_start) << std::endl;
				}
			}

			if (!output_prefix.empty() && iter && iter % dump_iter == 0)
			{
				Dump(sampler_, iter, thread_id);
			}
		}

		double total_end = lda::get_time();
		LOG(INFO) << "thread_id = " << thread_id <<" Total time for " << num_iterations << " iterations : " << (total_end - total_start) << " sec.";
	}

	int32_t LDAEngine::ReadData(const std::string& doc_file)
	{	
		data_block_->Read();
		//return data_block_->vocabs_size();
		return 0;
	}

	void LDAEngine::Dump(LightDocSampler &sampler_, int iter, int thread_id)
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

		// dump word-topic counts
		std::string word_topic_dump = doc_file +
			".word_topic.alpha_sum" + std::to_string(alpha_sum) + ".beta."
			+ std::to_string(beta) + ".mh_step." + std::to_string(mh_step) +
			+
			".iter." + std::to_string(iter) + ".vocab." +
			std::to_string(num_vocabs) + ".topic." + std::to_string(num_topics) + "." + output_prefix + ".thread." + std::to_string(thread_id);


		LOG(INFO) << "Writing output to " << word_topic_dump << " ...";

		sampler_.Dump(word_topic_dump, word_range_for_each_thread_[thread_id], word_range_for_each_thread_[thread_id + 1]);


		LOG(INFO) << "Dump completes";	
	}

}   // namespace lda
