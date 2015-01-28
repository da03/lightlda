#pragma once
#include <fstream>
#include <string>
#include <memory>
#include <fstream>
#include <string>
#include <map>
#include <cassert>
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <iostream>
#include <execinfo.h>

#define JUMP_(key, num_probes)    ( num_probes )

#define ILLEGAL_BUCKET -1

namespace lda
{
	class hybrid_map
	{
		friend class AliasSlice;
	public:
		hybrid_map()
			:memory_(nullptr),
			is_dense_(1),
			capacity_(0),
			empty_key_(0),
			deleted_key_(-1),
			num_deleted_key_(0), 
			key_(nullptr),
			value_(nullptr)
		{
			// CHECK(is_dense_) << "is_dense_ == 0";
		}
		hybrid_map(int32_t *memory, int32_t is_dense, int32_t capacity, int32_t *external_rehash_buf_)
			: memory_(memory),
			is_dense_(is_dense),
			capacity_(capacity),
			empty_key_(0),
			deleted_key_(-1),
			key_(nullptr),
			value_(nullptr), 
			num_deleted_key_(0), 
			external_rehash_buf_(external_rehash_buf_)
		{
			if (is_dense_ == 0) {
				key_ = memory_;
				value_ = memory_ + capacity_;
			}
		}

		hybrid_map(const hybrid_map &other)
		{
			this->memory_ = other.memory_;
			this->is_dense_ = other.is_dense_;
			this->capacity_ = other.capacity_;
			empty_key_ = other.empty_key_;
			deleted_key_ = other.deleted_key_;
			num_deleted_key_ = other.num_deleted_key_;
			external_rehash_buf_ = other.external_rehash_buf_;
			if (this->is_dense_)
			{
				this->key_ = nullptr;
				this->value_ = nullptr;
			}
			else
			{
				this->key_ = this->memory_;
				this->value_ = this->memory_ + capacity_;
			}
			
		}
		hybrid_map& operator=(const hybrid_map &other)
		{
			this->memory_ = other.memory_;
			this->is_dense_ = other.is_dense_;
			this->capacity_ = other.capacity_;
			empty_key_ = other.empty_key_;
			deleted_key_ = other.deleted_key_;
			num_deleted_key_ = other.num_deleted_key_;
			external_rehash_buf_ = other.external_rehash_buf_;
			if (this->is_dense_)
			{
				this->key_ = nullptr;
				this->value_ = nullptr;
			}
			else
			{
				this->key_ = this->memory_;
				this->value_ = this->memory_ + capacity_;
			}
			return *this;
		}

		inline void clear()
		{
			int32_t memory_size = is_dense_ ? capacity_ : 2 * capacity_;
			num_deleted_key_ = 0;
			memset(memory_, 0, memory_size * sizeof(int32_t));
		}

		inline int32_t nonzero_num() const
		{
			if (is_dense_)
			{
				int32_t size = 0;
				for (int i = 0; i < capacity_; ++i)
				{
					// if (memory_[i] > 0)
					if (memory_[i] != 0)
					{
						++size;
					}
				}
				return size;
			}
			else
			{
				int32_t size = 0;
				for (int i = 0; i < capacity_; ++i)
				{
					CHECK(key_ != NULL);
					if (key_[i] > 0)
					{
						++size;
					}
				}
				return size;
			}
		}

		inline void rehashing(/*int32_t *external_buf*/)
		{
			if (!is_dense_)
			{
				memcpy(external_rehash_buf_, memory_, 2 * capacity_ * sizeof(int32_t));
				int32_t *key = external_rehash_buf_;
				int32_t *value = external_rehash_buf_ + capacity_;
				memset(memory_, 0, 2 * capacity_ * sizeof(int32_t));
				for (int i = 0; i < capacity_; ++i)
				{
					if (key[i] > 0)
					{
						inc(key[i] - 1, value[i]);
					}
				}
				num_deleted_key_ = 0;
			}
		}

		/*
		inline void sorted_rehashing() {
			if (!is_dense_) 
			{
				std::map<int32_t, int32_t> rehash_buffer;
				for (int i = 0; i < capacity_; ++i) 
				{
					if (key_[i] > 0)
					{
						rehash_buffer[key_[i] - 1] = value_[i];
					}
				}
				memset(memory_, 0, 2 * capacity_ * sizeof(int32_t));
				for (auto it = rehash_buffer.begin();
					it != rehash_buffer.end(); ++it) 
				{
					inc(it->first, it->second);
				}
			}
		}
		*/

		inline void inc(int32_t key, int32_t delta)
		{
			// CHECK(is_dense_) << "is_dense = 0";
			// CHECK(key < capacity_) << "key >= capacity_";

			if (is_dense_)
			{
				CHECK(key < capacity_) << "key >= capacity_" << key <<" of " << capacity_;
				memory_[key] += delta;
				// CHECK_GE(memory_[key], 0);
			}
			else
			{
				int32_t internal_key = key + 1;
				std::pair<int32_t, int32_t> pos = find_position(internal_key);
				if (pos.first != ILLEGAL_BUCKET)
				{
					value_[pos.first] += delta;
					// CHECK_GE(value_[pos.first], 0);
					if (value_[pos.first] == 0)       // the value becomes zero, delete the key
					{
						key_[pos.first] = deleted_key_;
						++(num_deleted_key_); // num_deleted_key ++
						if (num_deleted_key_ * 50 > capacity_) {
							rehashing();
						}
					}
				}
				else                                 // not found the key, insert it with delta as value
				{
					key_[pos.second] = internal_key;
					value_[pos.second] = delta;
					// CHECK_GE(value_[pos.second], 0) << "key = " << key;
				}
			}
		}

		// query the value of |key|
		// if |key| is in the table, return the |value| corresonding to |key|
		// if not, just return 0
		inline int32_t operator[](int32_t key)
		{
			// CHECK(is_dense_) << "is_dense = 0";
			// CHECK(key < capacity_) << "key >= capacity_";

			if (is_dense_)
			{
				return memory_[key];
			}
			else
			{
				int32_t internal_key = key + 1;
				std::pair<int32_t, int32_t> pos = find_position(internal_key);
				if (pos.first != ILLEGAL_BUCKET)
				{
					return value_[pos.first];
				}
				else
				{
					return 0;
				}
			}
		}

		bool is_dense() { return is_dense_ == 1; }

		int32_t capacity() { return capacity_; }

		int32_t* memory() { return memory_; }

		int32_t* key() { return key_; }
		int32_t* value() { return value_; }

	public:
		size_t SerializedSize() const;
		size_t Serialize(void* bytes) const;
		void ApplySparseBatchInc(const void* data, size_t num_bytes);
		std::string DumpString() const {
			if (is_dense_) {
				std::string result;
				for (int i = 0; i < capacity_; ++i) {
					if (memory_[i] != 0) {
						result += std::to_string(i) + ":" + std::to_string(memory_[i]) + " ";
					}
				}
				return result;
			}
			else {
				std::string result;
				for (int i = 0; i < capacity_; ++i) {
					if (key_[i] > 0) {
						result += std::to_string(key_[i] - 1) + ":" + std::to_string(value_[i]) + " ";
					}
				}
				return result;
			}
		}
		std::string DebugString() const {
			if (is_dense_)
			{
				std::string result;
				for (int i = 0; i < capacity_; ++i)
				{
					//if (memory_[i] > 0)
					{
						result += std::to_string(memory_[i]) + " ";
					}
				}
				return result;
			}
			else
			{
				std::string result;
				for (int i = 0; i < capacity_; ++i)
				{
					CHECK(key_ != NULL);
					if (key_[i] > 0)
					{
						result += std::to_string(key_[i] - 1) + ":" + std::to_string(value_[i]) + " ";
					}
				}
				return result;
			}
		}
	private:
		inline std::pair<int32_t, int32_t> find_position(const int32_t key)
		{
			int num_probes = 0;
			int32_t capacity_minus_one = capacity_ - 1;
			//int32_t idx = hasher_(key) & capacity_minus_one;
            if (capacity_ <= 0) {
                void *array[10];
                  size_t size;

                    // get void*'s for all entries on the stack
                       size = backtrace(array, 10);
                    //
                    //     // print out all the frames to stderr
                           fprintf(stderr, "Error: signal :\n");
                             backtrace_symbols_fd(array, size, STDERR_FILENO);
                               exit(1);
            }
			int32_t idx = key % capacity_;
			int32_t insert_pos = ILLEGAL_BUCKET;
			while (1)                                           // probe until something happens
			{
				if (key_[idx] == empty_key_)                    // bucket is empty
				{
					if (insert_pos == ILLEGAL_BUCKET)           // found no prior place to insert
					{
						// LOG(INFO) << "Found empty key num_probes = " << num_probes;
						return std::pair<int32_t, int32_t>(ILLEGAL_BUCKET, idx);
					}
					else                                        // previously, there is a position to insert
					{
						// LOG(INFO) << "Found position to insert num_probes = " << num_probes;
						return std::pair<int32_t, int32_t>(ILLEGAL_BUCKET, insert_pos);
					}
				}
				else if (key_[idx] == deleted_key_)            // keep searching, but makr to insert
				{
					if (insert_pos == ILLEGAL_BUCKET)
					{
						insert_pos = idx;
					}
				}
				else if (key_[idx] == key)
				{
					// LOG(INFO) << "Found key num_probes = " << num_probes;
					return std::pair<int32_t, int32_t>(idx, ILLEGAL_BUCKET);
				}
				++num_probes;                                // we are doing another probe
				idx = (idx + JUMP_(key, num_probes) & capacity_minus_one);
				// if (num_probes >= capacity_) LOG(INFO) << "Hashtable is full: num_probes = " << num_probes;
				// CHECK(num_probes < capacity_ && "Hashtable is full: an error in key_equal<> or hash<>") << " Key = " << key << ". Num of non-zero = " << nonzero_num() << ". capacity = " << capacity_;
				if (num_probes >= capacity_) {
					LOG(INFO) << "Hashtable debug string " << DebugString();
					LOG(FATAL) << "Hashtable is full: an error in key_equal<> or hash<>"
						<< " Key = " << key << ". Num of non-zero = " << nonzero_num() 
						<< ". capacity = " << capacity_
						<< " deleted_key_num = " << num_deleted_key_;
				}
			}
		}

	private:

		int32_t *memory_;
		int32_t is_dense_;
		int32_t *key_;
		int32_t *value_;

		// if |is_dense_| == true, capactiy_ is the length of an array
		// if |is dense_| == false, capacity_ is the size of a light hash table
		int32_t capacity_;
		int32_t empty_key_;
		int32_t deleted_key_;

		int32_t num_deleted_key_;
		int32_t* external_rehash_buf_;
	};

}
