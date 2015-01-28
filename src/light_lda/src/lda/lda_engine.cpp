// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2014.03.29
// Modified: Gao Fei (v-feigao@microsoft.com
// Date: 2014.10.29

#include "lda/lda_engine.hpp"
#include <time.h>
#include <cstdlib>
#include <chrono>
#include <fstream>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <glog/logging.h>
//#include <Windows.h>
#include "lda/lda_stats.hpp"
#include "lda/light_doc_sampler.hpp"
#include "system/bg_workers.hpp"
#include "system/mem_transfer.hpp"
#include "system/ps_msgs.hpp"
#include "system/table_group.hpp"
#include "util/high_resolution_timer.hpp"
#include "util/serialized_row_reader.hpp"
#include "util/utils.hpp"

namespace lda {

	LDAEngine::LDAEngine() : thread_counter_(0), delta_thread_counter_(0)
	{
		util::Context& context = util::Context::get_instance();
		K_ = context.get_int32("num_topics");
		V_ = context.get_int32("num_vocabs");
		num_iterations_ = context.get_int32("num_iterations");
		num_threads_ = context.get_int32("num_worker_threads");
		num_delta_threads_ = context.get_int32("num_delta_threads"); // v-feigao: multi-delta threads
		compute_ll_interval_ = context.get_int32("compute_ll_interval");
		db_file_ = context.get_string("doc_file");
		vocab_file_ = context.get_string("vocab_file");
		num_blocks_ = context.get_int32("num_blocks");
		cold_start_ = context.get_bool("cold_start");

		data_.reset(new DataBlockBuffer(num_threads_));

		LOG(INFO) << "Construct model";
		word_topic_table_.reset(new WordTopicBuffer(num_threads_));
		summary_row_.reset(new SummaryBuffer(num_threads_));
		
		summary_row_->MutableWorkerBuffer().reset(new petuum::ClientSummaryRow(
			petuum::GlobalContext::kSummaryRowID, K_));
		summary_row_->MutableIOBuffer().reset(new petuum::ClientSummaryRow(
			petuum::GlobalContext::kSummaryRowID, K_));

		word_topic_delta_.reset(new DeltaSlice);
		summary_row_delta_.reset(new petuum::ClientSummaryRow(
			petuum::GlobalContext::kSummaryRowID, K_));

		vocabs_.resize(num_blocks_);
		delta_io_threads_.resize(num_delta_threads_); // v-feigao: multi-delta threads
		word_topic_delta_queues_.resize(num_delta_threads_);
		for (auto& queue : word_topic_delta_queues_)
			queue.reset(new WordTopicDeltaQueue);

		delta_pool_.Init(2 * num_threads_ * num_delta_threads_); // sizeof(DeltaArray) = 48MB, 256 * 48MB = 12GB
		summary_pool_.Init(2 * num_threads_);

		num_all_slice_ = 0;
		num_tokens_clock_ = 0;
		
		process_barrier_.reset(new boost::barrier(num_threads_));
		process_barrier_delta_.reset(new boost::barrier(num_delta_threads_)); // v-feigao: multi-delta threads
		process_barrier_all_.reset(new boost::barrier(num_threads_ + num_delta_threads_ + 2)); // v-feigao: multi-delta threads

		app_thread_running_ = true;
	}

	void LDAEngine::Setup()
	{
		util::Context& context = util::Context::get_instance();
		int32_t block_offset = context.get_int32("block_offset");

		for (int32_t id = 0; id < num_blocks_; ++id) 
		{
			std::string vocab_file_name = vocab_file_ + "." + std::to_string(id + block_offset);
			vocabs_[id].Read(vocab_file_name);
			num_all_slice_ += vocabs_[id].NumOfSlice();
			LOG(INFO) << "Block id = " << id << ". Num of slice = " << vocabs_[id].NumOfSlice();
		}

		LOG(INFO) << "Load locab vocabulary OK. Number of all slice = " << num_all_slice_ 
			<< ". Each batch has average number of slice = " 
			<< static_cast<double>(num_all_slice_) / num_blocks_;

		data_io_thread_ = std::thread(&LDAEngine::DataIOThreadFunc, this);
		model_io_thread_ = std::thread(&LDAEngine::ModelIOThreadFunc, this);
		for (auto& thread : delta_io_threads_) // v-feigao: multi-delta threads
			thread = std::thread(&LDAEngine::DeltaIOThreadFunc, this);
	}

	void LDAEngine::DataIOThreadFunc()
	{
		VLOG(0) << "Enter DataIOThreadFunc";
		util::Context& context = util::Context::get_instance();
		int32_t block_offset = context.get_int32("block_offset");
		int32_t iteration = context.get_int32("num_iterations");

		int32_t block_id = 0;
		std::string block_file = db_file_ + "." + std::to_string(block_id + block_offset);
		{
			BufferGuard<DataBlockBuffer> buffer_guard(*data_, 0);
			std::unique_ptr<LDADataBlock>& data_block = data_->MutableIOBuffer();
			double read_begin = lda::get_time();

			data_block->Read(block_file);

			double read_end = lda::get_time();
			LOG(INFO) << "Read time = " << read_end - read_begin << " seconds.";
		}

		process_barrier_all_->wait();
		
		for (int32_t iter = 0; num_blocks_ > 1 && iter <= iteration; ++iter) 
		{
			for (int32_t block_id = 0; block_id < num_blocks_ && app_thread_running_; ++block_id) 
			{
				BufferGuard<DataBlockBuffer> data_guard(*data_, 0);
				if (!app_thread_running_)
				{
					break;
				}

				std::unique_ptr<LDADataBlock>& data_block = data_->MutableIOBuffer();
				if (data_block->HasRead()) 
				{
					double write_begin = lda::get_time();
					data_block->Write();
					double write_end = lda::get_time();
					LOG(INFO) << "Write time = " << write_end - write_begin << " seconds.";
				}
				if (iter == iteration && block_id == num_blocks_ - 1)
					break;
				// Load New data;
				int32_t next_block_id = (block_id + 1) % num_blocks_;
				block_file = db_file_ + "." + std::to_string(next_block_id + block_offset);

				double read_begin = lda::get_time();
				data_block->Read(block_file);
				double read_end = lda::get_time();
				LOG(INFO) << "Read time = " << read_end - read_begin << " seconds.";
			}
		}
		VLOG(0) << "Exit DataIOThreadFunc";
	}

	void LDAEngine::ModelIOThreadFunc() 
	{
		VLOG(0) << "Enter ModelIOThreadFunc";
		petuum::TableGroup::RegisterThread();
		process_barrier_all_->wait();

		util::Context& context = util::Context::get_instance();
		int32_t staleness = context.get_int32("staleness");
		int32_t iteration = context.get_int32("num_iterations");

		petuum::VectorClock server_vector_clock;
		for (auto&server_id : petuum::GlobalContext::get_server_ids())
			server_vector_clock.AddClock(server_id);

		VLOG(0) << "Model IO Begin work";

		int32_t count = 0;

		for (int32_t iter = 0; iter < iteration; ++iter) 
		{
			// SSP
			petuum::TableGroup::WaitServer(iter, staleness);

			for (int32_t batch_id = 0; app_thread_running_ && batch_id < num_blocks_; ++batch_id) 
			{
				LocalVocab& local_vocab = vocabs_[batch_id];
				for (int32_t slice_id = 0; app_thread_running_ && 
					slice_id < local_vocab.NumOfSlice(); ++slice_id) 
				{

					BufferGuard<WordTopicBuffer> word_topic_table_guard(*word_topic_table_, 0);
					BufferGuard<SummaryBuffer> summary_row_guard(*summary_row_, 0);

					std::unique_ptr<ModelSlice>& word_topic_table =
						word_topic_table_->MutableIOBuffer();
					std::unique_ptr<petuum::ClientSummaryRow>& summary_row =
						summary_row_->MutableIOBuffer();

					util::MtQueueMove<std::unique_ptr<petuum::ServerPushOpLogIterationMsg>> 
						*server_model_slice_queue = petuum::TableGroup::GetServerDeltaQueue();

					//(v-feigao) : new model slice request msg
					RequestModelSlice(slice_id, local_vocab);
					int64_t global_tf_sum = local_vocab.GlobalTFSum(slice_id);
					int64_t local_tf_sum = local_vocab.LocalTFSum(slice_id);

					word_topic_table->Init(&local_vocab, slice_id);
					summary_row->Reset();

					bool word_topic_table_clock = false;
					bool summary_row_clock = false;

					//update the model slice based on the ServerModelSliceRequestReply message.
					VLOG(0) << "Wait server reply";
					int64_t model_size = 0;
					int64_t num_nonzero_entries = 0;
					while (true) 
					{
						std::unique_ptr<petuum::ServerPushOpLogIterationMsg> msg_ptr;
						if (!server_model_slice_queue->Pop(msg_ptr)) 
						{
							break;
						}

						if (msg_ptr->get_table_id() == petuum::GlobalContext::kWordTopicTableID) 
						{
							model_size += msg_ptr->get_size();
							num_nonzero_entries += word_topic_table->ApplyServerModelSliceRequestReply(*msg_ptr);
							if (msg_ptr->get_is_clock()) 
							{
								int32_t new_clock = server_vector_clock.Tick(msg_ptr->get_server_id());
								if (new_clock) 
								{
									word_topic_table_clock = true;
								}
							}
						}
						else if (msg_ptr->get_table_id() == petuum::GlobalContext::kSummaryRowID) 
						{
							summary_row->ApplyServerModelSliceRequestReply(*msg_ptr);
							if (msg_ptr->get_is_clock()) 
							{
								summary_row_clock = true;
							}
						}
						else 
						{
							LOG(FATAL) << "Incorrect table id, table id = " << msg_ptr->get_table_id();
						}

						if (word_topic_table_clock && summary_row_clock) 
						{
							break;
						}
					}
					LOG(INFO) << "ModelIO: Model Slice, batch id = " << batch_id 
						<< ". slice_id = " << slice_id << " is fine now!" << " size = " << model_size
						<< ". Non-zero entries = " << num_nonzero_entries;
					LOG(INFO) << "Global TF sum = " << global_tf_sum;
					LOG(INFO) << "Local TF sum = " << local_tf_sum;
				} // end for slice_id
			} // end for batch_id
		}// end while

		petuum::TableGroup::WaitServer(iteration, 0);
		petuum::TableGroup::DeregisterThread();
		VLOG(0) << "Exit ModelIOThreadFunc";
	}

	void LDAEngine::DeltaIOThreadFunc() 
	{
		VLOG(0) << "Enter DeltaIOThreadFunc";
		int32_t delta_thread_id = delta_thread_counter_++; // v-feigao: multi-delta threads

		if (delta_thread_id == 0) // v-feigao: multi-delta threads
			petuum::TableGroup::RegisterThread(); // only one thread serialize and send

		process_barrier_all_->wait();
		delta_inited_ = false;
		process_barrier_delta_->wait();

		std::unique_ptr<WordTopicDeltaQueue>& word_topic_delta_queue = word_topic_delta_queues_[delta_thread_id];

		int num_delta = 0;

		petuum::VectorClock app_vector_clock;
		for (int32_t app_thread = 1; app_thread <= num_threads_; ++app_thread) 
		{
			app_vector_clock.AddClock(app_thread);
		}

		int32_t clock_num = 0;

		double delta_merge_time = 0.0;
		int32_t delta_counter = 0;
		int32_t iter = 0;
		while (true)
		{
			std::unique_ptr<petuum::DeltaArray> curr_word_topic_delta;

            //LOG(INFO)<<"DeltaIO enters iter";

			if (!word_topic_delta_queue->Pop(curr_word_topic_delta))
				break;
            //LOG(INFO)<<"DeltaIO pops";

			num_delta_clock_ += curr_word_topic_delta->index_;

			int32_t batch_id = curr_word_topic_delta->BatchID();
			int32_t slice_id = curr_word_topic_delta->SliceID();
			LocalVocab& local_vocab = vocabs_[batch_id];

            //LOG(INFO)<<"DeltaIO delta_inited: "<<delta_inited_;
			while (!delta_inited_) 
			{
				std::lock_guard<std::mutex> lock_guard(delta_mutex_);
				if (delta_inited_) break;
				word_topic_delta_->Init(&local_vocab, slice_id);
				delta_inited_ = true;
			}
            //LOG(INFO)<<"==here1DeltaIO pops";

			petuum::HighResolutionTimer merge_timer;
			word_topic_delta_->MergeFrom(*curr_word_topic_delta);
			delta_merge_time += merge_timer.elapsed();
			++delta_counter;
            //LOG(INFO)<<"==here2DeltaIO pops";
			
			std::unique_ptr<petuum::SummaryDelta> curr_summary_row_delta;
			if (delta_thread_id == 0) // v-feigao: only thread 0 care about summary delta	
			{ 
				summary_delta_queue_.Pop(curr_summary_row_delta);
				summary_row_delta_->MergeFrom(*curr_summary_row_delta);
			}
            //LOG(INFO)<<"==here3DeltaIO pops";

			if (curr_word_topic_delta->Clock())
			{
            //LOG(INFO)<<"==here4DeltaIO pops";
				int32_t new_clock = app_vector_clock.Tick(curr_word_topic_delta->ThreadId());
				if (new_clock) 
				{
					process_barrier_delta_->wait();
					if (delta_thread_id == 0) {
						bool is_iteration_clock = false;

						if (++clock_num == num_all_slice_) 
						{
							is_iteration_clock = true;
							clock_num = 0;
						}

						summary_row_delta_->ClientCreateSendTableDeltaMsg(DeltaSendMsg);

						petuum::HighResolutionTimer delta_send_timer;
						int64_t nonzero_entries = 
							word_topic_delta_->ClientCreateSendTableDeltaMsg(DeltaSendMsg, is_iteration_clock);
						LOG(INFO) << "Word topic table stat: numner = " << delta_counter
							<< ". merge_time = " << delta_merge_time
							<< ". send time = " << delta_send_timer.elapsed();

						LOG(INFO) << "Num of delta entries = " << num_delta_clock_ << "\t Num of aggregated nonzero entries = " << nonzero_entries;
						num_delta_clock_ = 0;
						summary_row_delta_->Reset();
						delta_inited_ = false;
					}
					delta_merge_time = 0.0; delta_counter = 0;
					++iter;
					process_barrier_delta_->wait();
				}
			}
            //LOG(INFO)<<"==here5DeltaIO pops";
            //LOG(INFO)<<"DeltaIO changes delta_poop_";
			delta_pool_.Free(curr_word_topic_delta);
			if (delta_thread_id == 0) summary_pool_.Free(curr_summary_row_delta);
		}
        //LOG(INFO)<<"DeltaIO prepares to deregister";	
		if (delta_thread_id == 0) {
			petuum::TableGroup::DeregisterThread();
		}
		VLOG(0) << "Exit DeltaIOThreadFunc";
	}

	void LDAEngine::DeltaSendMsg(int32_t server_id, 
		petuum::ClientSendOpLogIterationMsg* msg, 
		bool is_clock, 
		bool is_iteration_clock) 
	{
		msg->get_is_clock() = is_clock;
		msg->get_server_id() = server_id;
		msg->get_is_iteration_clock() = is_iteration_clock;
		int32_t client_id = petuum::GlobalContext::get_client_id();
		msg->get_client_id() = client_id;
		int32_t bg_id = petuum::GlobalContext::get_head_bg_id(client_id);
		
		size_t sent_size = (petuum::GlobalContext::comm_bus->SendInProc)(bg_id, msg->get_mem(),
			msg->get_size());
		VLOG(0) << "Delta IO send table delta msg. table_id = " << msg->get_table_id() 
			<< " size = " << sent_size;
		CHECK_EQ(sent_size, msg->get_size());
	}

	void LDAEngine::RequestModelSlice(int32_t slice_id, const LocalVocab& local_vocab)
	{
		int32_t avai_size = local_vocab.MsgSize(slice_id);
		VLOG(0) << "Create request model slice msg. size = " << avai_size;
		petuum::ClientModelSliceRequestMsg* msg = new petuum::ClientModelSliceRequestMsg(avai_size);
		int32_t client_id = petuum::GlobalContext::get_client_id();
		msg->get_client_id() = client_id;
		local_vocab.SerializeAs(msg->get_data(), avai_size, slice_id);

		int32_t bg_id = petuum::GlobalContext::get_head_bg_id(client_id);
		VLOG(0) << "Delta IO send RequestModelSlice msg.";
		size_t sent_size = (petuum::GlobalContext::comm_bus->SendInProc)(bg_id, msg->get_mem(),
			msg->get_size());
		CHECK_EQ(sent_size, msg->get_size());
	}

	void LDAEngine::Train()
	{
		// Initialize local thread data structures.
		int thread_id = ++thread_counter_;
		VLOG(0) << "Enter AppThread = " << thread_id;
		
		long long maskLL = 0;
		maskLL |= (1LL << (thread_id));
		// ORIG: DWORD_PTR mask = maskLL;

		// ORIG: SetThreadAffinityMask(GetCurrentThread(), mask);

		process_barrier_all_->wait();

		LightDocSampler sampler;
		LDAStats lda_stats;

		wood::xorshift_rng& rng = sampler.rng();

		int iter = 0;

		std::vector<std::unique_ptr<petuum::DeltaArray>> word_topic_delta_vec(num_delta_threads_); 
		std::unique_ptr<petuum::SummaryDelta> summary_delta;   
		for (auto& word_topic_delta : word_topic_delta_vec)
			delta_pool_.Allocate(word_topic_delta);
		summary_pool_.Allocate(summary_delta);

		// pass the whole data, init the model. The initialization is seen ase the Iter 0;
		// LOG(INFO) << "Begin topic initialization in thread = " << thread_id << std::flush;
		int num_doc = 0;
		int local_pass = 0;

		// initialization .
		{
			for (int batch_id = 0; batch_id < num_blocks_; ++batch_id)
			{
				if (num_blocks_ > 1) data_->Start(thread_id);

				std::unique_ptr<LDADataBlock> &lda_data_block = data_->MutableWorkerBuffer();

				if (!lda_data_block->HasRead()) 
				{
					LOG(FATAL) << "Invalid data block";
				}
				process_barrier_->wait();
				LocalVocab& local_vocab = vocabs_[batch_id];
				int32_t num_of_slice = local_vocab.NumOfSlice();
                LOG(ERROR)<<"num of slice: " << num_of_slice;
				for (int32_t slice_id = 0; slice_id < num_of_slice; ++slice_id)
				{
					process_barrier_->wait();					
					petuum::HighResolutionTimer iter_timer;
					int32_t num_tokens = 0;
					int32_t doc_begin = lda_data_block->Begin(thread_id - 1);
					int32_t doc_end = lda_data_block->End(thread_id - 1);
					for (int32_t doc_index = doc_begin;
						doc_index != doc_end;
						++doc_index) 
					{
						std::shared_ptr<LDADocument> doc = lda_data_block->GetOneDoc(doc_index);
						for (int32_t i = 0; i < word_topic_delta_vec.size(); ++i) 
						{
							auto& word_topic_delta = word_topic_delta_vec[i];
							
							if (!word_topic_delta->ValidDocSize(doc->size()))
							{
								word_topic_delta->SetProperty(thread_id, iter, batch_id, slice_id, false);
								word_topic_delta_queues_[i]->Push(word_topic_delta);
								CHECK(!word_topic_delta.get()) << "unique Pointer should not own memory";
								delta_pool_.Allocate(word_topic_delta);
								if (i == 0)
								{
									summary_delta_queue_.Push(summary_delta);
									summary_pool_.Allocate(summary_delta);
								}
							}
						}
						int32_t slice_last_word = local_vocab.LastWord(slice_id);
						int32_t& cursor = doc->get_cursor();
						if (slice_id == 0) cursor = 0;
						for ( ; cursor != doc->size(); ++cursor)
						{
							int32_t word = doc->Word(cursor);
							if (word > slice_last_word)
								break;
							
							if (cold_start_) 
							{
								int32_t topic = rng.rand_k(K_);
								doc->SetTopic(cursor, topic); 
							}
														
							++num_tokens;
							int32_t shard_id = word % num_delta_threads_;
							word_topic_delta_vec[shard_id]->Update(word, doc->Topic(cursor), 1);
							summary_delta->Update(doc->Topic(cursor), 1);
						}
					}
					num_tokens_clock_ += num_tokens;
					if (thread_id == 1)
					{
						LOG(INFO) << "Init the slice " << slice_id << " on data " << batch_id <<
							" . Tokens Num in one thread: " << num_tokens << ". Took time : " << iter_timer.elapsed();
					}
					//LOG(ERROR)<<"1---here"<<thread_id<<": "<<slice_id;
					summary_delta_queue_.Push(summary_delta);
					//LOG(ERROR)<<"1.5---here"<<thread_id<<": "<<slice_id;
					summary_pool_.Allocate(summary_delta);
					//LOG(ERROR)<<"2---here"<<thread_id<<": "<<slice_id;
					for (int32_t i = 0; i < word_topic_delta_vec.size(); ++i)
					{
						auto& word_topic_delta = word_topic_delta_vec[i];
						
						word_topic_delta->SetProperty(thread_id, iter, batch_id, slice_id, true);
						word_topic_delta_queues_[i]->Push(word_topic_delta);
						delta_pool_.Allocate(word_topic_delta);
					}	
					//LOG(ERROR)<<"3---here"<<thread_id<<": "<<slice_id;
					
					process_barrier_->wait();
				}
				process_barrier_->wait();
				if (num_blocks_ > 1) data_->End(thread_id);
			} // end while
		} // end initialization

		if (thread_id == 1 && compute_ll_interval_ != -1 && iter % compute_ll_interval_ == 0)
		{
			LOG(INFO) << "Sample token numner = " << num_tokens_clock_;
			num_tokens_clock_ = 0;
			doc_likelihood_ = 0;
			word_likelihood_ = 0;
		}
		process_barrier_->wait();
		VLOG(0) << "End topic initialization in thread = " << thread_id << " with local_pass = " << local_pass;
		process_barrier_->wait();
		util::Context& context = util::Context::get_instance();
		int32_t client_id = petuum::GlobalContext::get_client_id();
		int num_iterations = context.get_int32("num_iterations");
		petuum::HighResolutionTimer total_timer;
		// main gibbs sampling

		double elapsed_time = 0.0;
		double elapsed_worker_time = 0.0; // time for gibbs sampling
		double elapsed_wait_time = 0.0;   // time for wait model prefetch
		double elapsed_alias_time = 0.0;  // time for generate alias table

		for (iter = 1; iter <= num_iterations; ++iter)
		{
			// for every data batch
			doc_likelihood_ = 0.0;
			word_likelihood_ = 0.0;
			process_barrier_->wait();
			for (int batch_id = 0; batch_id < num_blocks_; ++batch_id) {
				// Get access of data batch
				// BufferGuard<DataBlockBuffer> data_guard(*data_, thread_id);
				if (num_blocks_ > 1) data_->Start(thread_id);
				std::unique_ptr<LDADataBlock> &lda_data_block = data_->MutableWorkerBuffer();

				LocalVocab& local_vocab = vocabs_[batch_id];
				int32_t num_of_slice = local_vocab.NumOfSlice();
				// for every model slice
				for (int32_t slice_id = 0; slice_id < num_of_slice; ++slice_id)
				{
					petuum::HighResolutionTimer wait_timer;
					BufferGuard<WordTopicBuffer> word_topic_table_guard(*word_topic_table_, thread_id);
					BufferGuard<SummaryBuffer> summary_row_guard(*summary_row_, thread_id);
					double wait_time = wait_timer.elapsed();
					process_barrier_->wait();

					petuum::HighResolutionTimer alias_timer;
					std::unique_ptr<ModelSlice>& word_topic_table =
						word_topic_table_->MutableWorkerBuffer();
					std::unique_ptr<petuum::ClientSummaryRow>& summary_row =
						summary_row_->MutableWorkerBuffer();
					process_barrier_->wait();
					if (thread_id == 1) alias_slice_.Init(&local_vocab, slice_id);
					process_barrier_->wait();
					// each thread generate a slice of alias table;

					// FOR: test doc proposal
					alias_slice_.GenerateAliasTable(*word_topic_table, *summary_row, thread_id - 1, rng);
					VLOG(0) << "Thread " << thread_id << "Finish Generate Alias Table";

					process_barrier_->wait();
					// return;
					double alias_time = alias_timer.elapsed();
					
					// model if fine now...
					// sample on this model slice
					petuum::HighResolutionTimer worker_timer;
					if (!lda_data_block->HasRead()) 
					{
						LOG(FATAL) << "Invalid data block";
					}
					VLOG(0) << "Thread id = " << thread_id << " sample data batch = " << batch_id
						<< " on model slice = " << slice_id;

					int32_t doc_begin = lda_data_block->Begin(thread_id - 1);
					int32_t doc_end = lda_data_block->End(thread_id - 1);
					// sampler.zero_statistics();
					for (int32_t doc_index = doc_begin;	doc_index != doc_end; ++doc_index) 
					{
						std::shared_ptr<LDADocument> doc = lda_data_block->GetOneDoc(doc_index);
						for (int32_t i = 0; i < word_topic_delta_vec.size(); ++i) 
						{
							auto& word_topic_delta = word_topic_delta_vec[i];
							auto& word_topic_delta_queue = word_topic_delta_queues_[i];
							if (!word_topic_delta->ValidDocSize(doc->size())) 
							{
								word_topic_delta->SetProperty(thread_id, iter, batch_id, slice_id, false);
								word_topic_delta_queue->Push(word_topic_delta);
								CHECK(!word_topic_delta.get()) << "unique Pointer should not own memory";
								delta_pool_.Allocate(word_topic_delta);
								if (i == 0) 
								{
									summary_delta_queue_.Push(summary_delta);
									summary_pool_.Allocate(summary_delta);
								}
							}
						}

						num_tokens_clock_ += sampler.SampleOneDoc(
							doc.get(), *word_topic_table, *summary_row, alias_slice_, word_topic_delta_vec, *summary_delta);

					}
					// sampler.print_statistics();

					for (int32_t i = 0; i < word_topic_delta_vec.size(); ++i)
					{
						auto& word_topic_delta = word_topic_delta_vec[i];
						auto& word_topic_delta_queue = word_topic_delta_queues_[i];
						word_topic_delta->SetProperty(thread_id, iter, batch_id, slice_id, true);
						word_topic_delta_queue->Push(word_topic_delta);
						delta_pool_.Allocate(word_topic_delta);
					}
					summary_delta_queue_.Push(summary_delta);
					summary_pool_.Allocate(summary_delta);
					
					process_barrier_->wait();
					
					if (thread_id == 1)
					{
						double worker_time = worker_timer.elapsed();
						double epoch_time = wait_time + worker_time + alias_time;
						elapsed_time += epoch_time;
						elapsed_wait_time += wait_time;
						elapsed_alias_time += alias_time;
						elapsed_worker_time += worker_time;
						
						LOG(INFO) << "Iter: " << iter << "\tClient: " << thread_id
							<< "\tDataBatch: " << batch_id << "\tSlice: " << slice_id;
						LOG(INFO) << "wait time: " << wait_time
							<< "\talias time: " << alias_time 
							<< "\twork time: " << worker_time
							<< "\ttotal time: " << epoch_time 
							<< "\telapsed time: " << elapsed_time;
						LOG(INFO) << "Sample token number = " << num_tokens_clock_;
						LOG(INFO) << "Sampling Thread Throughput: "
							<< static_cast<double>(num_tokens_clock_ / num_threads_ / worker_time)
							<< " tokens/(thread*sec)"
							<< "\tSampling Client Throughput: "
							<< static_cast<double>(num_tokens_clock_ / worker_time)
							<< " tokens/sec"
							<< "\tThread Throughput: "
							<< static_cast<double>(num_tokens_clock_ / num_threads_ / epoch_time)
							<< " tokens/(thread*sec)"
							<< "\tClient Throughput: "
							<< static_cast<double>(num_tokens_clock_  / epoch_time)
							<< " tokens/sec";
						num_tokens_clock_ = 0;
					}
					process_barrier_->wait();

					// likelihood
					lda_stats.Init(&local_vocab, slice_id);
					process_barrier_->wait();
					
					if (compute_ll_interval_ != -1 && iter % compute_ll_interval_ == 0) 
					{
						process_barrier_->wait();
						double thread_doc_likelihood = 0.0;
						double thread_word_likelihood = 0.0;
						if (slice_id == 0) { // Compute doc llh when slice_id == 0
							int32_t doc_begin = lda_data_block->Begin(thread_id - 1);
							int32_t doc_end = lda_data_block->End(thread_id - 1);
							for (int32_t doc_index = doc_begin, doc_num = 0;
								doc_index != doc_end;
								++doc_index) {
								std::shared_ptr<LDADocument> doc = lda_data_block->GetOneDoc(doc_index);
								thread_doc_likelihood += lda_stats.ComputeOneDocLLH(doc.get());
								if (++doc_num >= 10000) break;
							}
						}
						// word_likelihood
						
						if (batch_id == 0)
						{// Compute word llh when batch_id == 0
							thread_word_likelihood += lda_stats.ComputeOneSliceWordLLH(*word_topic_table, thread_id-1);
						}
						if (thread_id == 1 && slice_id == 0 && batch_id == 0)
						{
							double normal_llh =  lda_stats.NormalizeWordLLH(*summary_row);
							thread_word_likelihood += normal_llh;
							LOG(INFO) << "Normalize likelihood = " << normal_llh;
						}
						{
							std::lock_guard<std::mutex> lock_guard(llh_mutex_);
							doc_likelihood_ += thread_doc_likelihood;
							word_likelihood_ += thread_word_likelihood;
						}
					}
					process_barrier_->wait();
				}
				process_barrier_->wait();
				if (num_blocks_ > 1) data_->End(thread_id);
			} // end while
			if (thread_id == 1)
				LOG(INFO) << "Iter: " << iter
				<< " Elapsed_wait time = " << elapsed_wait_time
				<< " Elapsed_alias time = " << elapsed_alias_time
				<< " Elapsed_worker time = " << elapsed_worker_time
				<< " Elapsed time = " << elapsed_time;
			if (thread_id == 1 && compute_ll_interval_ != -1 && iter % compute_ll_interval_ == 0) {
				LOG(INFO) << " Doc likelihood = " << doc_likelihood_
					<< " Word Likelihood = " << word_likelihood_;
				doc_likelihood_ = 0;
				word_likelihood_ = 0;
			}
			process_barrier_->wait();
		}

		process_barrier_->wait();
		LOG(INFO) << "Thread " << thread_id << "finish gibbs sampling";

		// finish, notify and wait the io threads to exit

		if (thread_id == 1)
		{
			app_thread_running_ = false;
			data_->Exit();
			util::MtQueueMove<std::unique_ptr<petuum::ServerPushOpLogIterationMsg>> *server_delta_queue
				= petuum::TableGroup::GetServerDeltaQueue();
			server_delta_queue->Exit();
			

			LOG(INFO) << "App thread send finish msg";
			data_io_thread_.join();
			model_io_thread_.join();
			for (auto& word_topic_delta_queue : word_topic_delta_queues_)
			  word_topic_delta_queue->Exit();
			for (auto& thread : delta_io_threads_)
				thread.join();
		}

		LOG(INFO) << "Exit AppThread = " << thread_id;
	}

}   // namespace lda
