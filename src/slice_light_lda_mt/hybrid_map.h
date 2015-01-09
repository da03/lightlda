#pragma once
#include <fstream>
#include <string>
#include <memory>
#include <fstream>
#include <string>
#include <cassert>
#include <glog\logging.h>
#include <gflags\gflags.h>
#include "alias_multinomial_rng_int.hpp"

#define JUMP_(key, num_probes)    ( num_probes )

#define ILLEGAL_BUCKET -1

namespace lda
{
	
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
	};

	class hybrid_alias_map;

	class hybrid_map
	{
		friend class hybrid_alias_map;
	public:
		hybrid_map()
			:memory_(nullptr),
			is_dense_(1),
			capacity_(0),
			empty_key_(0),
			deleted_key_(-1),
			key_(nullptr),
			value_(nullptr),
			num_deleted_key_(0),
			external_rehash_buf_(nullptr)
		{
			// CHECK(is_dense_) << "is_dense_ == 0";
		}
		hybrid_map(int32_t *memory, int32_t is_dense, int32_t capacity, int32_t num_deleted_key
			, int32_t *external_rehash_buf_)
			: memory_(memory),
			is_dense_(is_dense),
			capacity_(capacity),
			empty_key_(0),
			deleted_key_(-1),
			key_(nullptr),
			value_(nullptr),
			num_deleted_key_(num_deleted_key),
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
			memset(memory_, 0, memory_size * sizeof(int32_t));
		}

		inline int32_t nonzero_num() const
		{
			if (is_dense_)
			{
				int32_t size = 0;
				for (int i = 0; i < capacity_; ++i)
				{
					if (memory_[i] > 0)
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
					if (key_[i] > 0)
					{
						++size;
					}
				}
				return size;
			}
		}

		inline bool is_dense() const
		{
			return is_dense_;
		}

		inline int32_t capacity() const
		{
			return capacity_;
		}

		inline int32_t *memory() const
		{
			return memory_;
		}
		inline int32_t* key() const
		{
			return key_;
		}
		inline int32_t* value() const
		{
			return value_;
		}

		inline void rehashing()
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

		inline void inc(int32_t key, int32_t delta)
		{
			if (is_dense_)
			{
				memory_[key] += delta;
			}
			else
			{
				int32_t internal_key = key + 1;
				std::pair<int32_t, int32_t> pos = find_position(internal_key);
				if (pos.first != ILLEGAL_BUCKET)
				{
					value_[pos.first] += delta;
					if (value_[pos.first] == 0)       // the value becomes zero, delete the key
					{
						key_[pos.first] = deleted_key_;

						++num_deleted_key_;        // num_deleted_key ++
						if (num_deleted_key_ * 20 > capacity_) {
							rehashing();
						}
					}
				}
				else                                 // not found the key, insert it with delta as value
				{
					key_[pos.second] = internal_key;
					value_[pos.second] = delta;
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
	
	private:
		inline std::pair<int32_t, int32_t> find_position(const int32_t key) const
		{
			int num_probes = 0;
			int32_t capacity_minus_one = capacity_ - 1;
			//int32_t idx = hasher_(key) & capacity_minus_one;
			int32_t idx = key % capacity_;
			int32_t insert_pos = ILLEGAL_BUCKET;
			while (1)                                           // probe until something happens
			{
				if (key_[idx] == empty_key_)                    // bucket is empty
				{
					if (insert_pos == ILLEGAL_BUCKET)           // found no prior place to insert
					{
						return std::pair<int32_t, int32_t>(ILLEGAL_BUCKET, idx);
					}
					else                                        // previously, there is a position to insert
					{
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
					return std::pair<int32_t, int32_t>(idx, ILLEGAL_BUCKET);
				}
				++num_probes;                                // we are doing another probe
				idx = (idx + JUMP_(key, num_probes) & capacity_minus_one);
				// assert(num_probes < capacity_ && "Hashtable is full: an error in key_equal<> or hash<>");
				CHECK(num_probes < capacity_ && "Hashtable is full: an error in key_equal<> or hash<>") << " Key = " << key << ". Num of non-zero = " << nonzero_num();
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

	class hybrid_alias_map
	{
	public:

		hybrid_alias_map()
			:memory_(nullptr),
			is_dense_(1),
			kv_(nullptr),
			idx_(nullptr),
			capacity_(0),
			size_(0),
			mass_(0),
			n_kw_mass_(0.0),
			beta_mass_(0.0)
		{
			// CHECK(is_dense_) << "is_dense_ == 0";
		}
		hybrid_alias_map(int32_t *memory, int32_t is_dense, int32_t capacity)
			:memory_(memory),
			is_dense_(is_dense),
			capacity_(capacity),
			kv_(nullptr),
			idx_(nullptr),
			size_(0),
			mass_(0),
			n_kw_mass_(0.0),
			beta_mass_(0.0)
		{
			if (is_dense_)
			{
				kv_ = memory_;
				idx_ = nullptr;
			}
			else
			{
				kv_ = memory_;
				idx_ = memory_ + capacity_ * 2;
			}
		}

		hybrid_alias_map(const hybrid_alias_map &other)
		{
			this->memory_ = other.memory_;
			this->is_dense_ = other.is_dense_;
			this->capacity_ = other.capacity_;

			this->kv_ = other.kv_;
			this->idx_ = other.idx_;
			this->height_ = other.height_;
			this->size_ = other.size_;

			this->mass_ = other.mass_;
			this->n_kw_mass_ = other.n_kw_mass_;
			this->beta_mass_ = other.beta_mass_;
		}
		hybrid_alias_map& operator=(const hybrid_alias_map &other)
		{
			this->memory_ = other.memory_;
			this->is_dense_ = other.is_dense_;
			this->capacity_ = other.capacity_;

			this->kv_ = other.kv_;
			this->idx_ = other.idx_;
			this->height_ = other.height_;
			this->size_ = other.size_;

			this->mass_ = other.mass_;
			this->n_kw_mass_ = other.n_kw_mass_;
			this->beta_mass_ = other.beta_mass_;

			return *this;
		}

		inline void clear()
		{
			size_ = 0;
		}

		inline int32_t size() const
		{
			return size_;
		}

		inline void build_table(
			wood::AliasMultinomialRNGInt &alias_rng,
			const hybrid_map &word_topic_row,
			const std::vector<int64_t> &summary_row,
			std::vector<float> &q_w_proportion,
			float beta,
			float beta_sum)
		{
			if (is_dense_)
			{
				size_ = capacity_;
				// CHECK(capacity_ <= 1000000);
				mass_ = 0;
				for (int k = 0; k < capacity_; ++k)
				{
					int32_t n_kw = word_topic_row.memory_[k];
					float prop = (n_kw + beta) / (summary_row[k] + beta_sum);
					q_w_proportion[k] = prop;
					mass_ += prop;
				}
				alias_rng.SetProportionMass(q_w_proportion, mass_, memory_, &height_);
			}
			else
			{
				size_ = 0;
				n_kw_mass_ = 0;
				int32_t row_capacity = word_topic_row.capacity_;
				// CHECK(row_capacity <= 1000000);
				for (int k = 0; k < row_capacity; ++k)
				{
					int32_t key = word_topic_row.key_[k];
					if (key > 0)
					{
						int32_t n_kw = word_topic_row.value_[k];
						//float prop = n_kw / (summary_row[k] + beta_sum);
						float prop = n_kw / (summary_row[key - 1] + beta_sum);
						q_w_proportion[size_] = prop;
						idx_[size_] = word_topic_row.key_[k] - 1;   // minus one from the the internal key
						// memory_[3 * size_ + 2] = word_topic_row.key_[k] - 1;
						n_kw_mass_ += prop;
						++size_;
					}
				}
				// CHECK(size_ != 0);
				if (size_ == 0)
				{
					// it is possible that, the local tf of a word is zero
					return;
				}
				alias_rng.SetProportionMass(q_w_proportion, size_, n_kw_mass_, memory_, &height_);
			}
		}
		inline int32_t next(wood::xorshift_rng &rng, int32_t beta_height, float beta_mass, std::vector<wood::alias_k_v> &beta_k_v)
		{
			if (is_dense_)
			{
				auto sample = rng.rand();
				int idx = sample / height_;
				int32_t *p = memory_ + 2 * idx;
				int32_t k = *p;
				p++;
				int32_t v = *p;
				int32_t m = -(sample < v);
				return (idx & m) | (k & ~m);
			}
			else
			{
				float sample = rng.rand_real() * (n_kw_mass_ + beta_mass);
				if (sample < n_kw_mass_)
				{
					/*
					auto n_kw_sample = rng.rand();
					int32_t idx = n_kw_sample / height_;
					int32_t *p = memory_ + 2 * idx;
					int32_t k = *p; p++;
					int32_t v = *p;
					int32_t id = idx_[idx];

					
					// return n_kw_sample < v ? id : k;
					int32_t m = -(n_kw_sample < v);
					return (id & m) | (k & ~m);
					*/
					auto n_kw_sample = rng.rand();
					int32_t idx = n_kw_sample / height_;
					int32_t *p = memory_ + 2 * idx;
					int32_t k = *p; p++;
					int32_t v = *p;
					int32_t id = idx_[idx];
					int32_t k_id = idx_[k];


					// return n_kw_sample < v ? id : k_id;
					int32_t m = -(n_kw_sample < v);
					return (id & m) | (k_id & ~m);

				}
				else
				{
					auto sample = rng.rand();
					int idx = sample / beta_height;
					int32_t k = beta_k_v[idx].k_;
					int32_t v = beta_k_v[idx].v_;
					int32_t m = -(sample < v);
					return (idx & m) | (k & ~m);
				}
			}
		}

	private:
		int32_t *memory_;
		int32_t is_dense_;
		int32_t *kv_;
		int32_t *idx_;
		int32_t height_;
		int32_t capacity_;
		int32_t size_;

		float mass_;
		float n_kw_mass_;
		float beta_mass_;
		// static std::vector<wood::alias_k_v> beta_kv_;
	};

	class LDAModelBlock
	{
	public:
		LDAModelBlock();
		~LDAModelBlock();

		inline hybrid_map get_row(int word_id, int32_t *external_buf)
		{
			hybrid_map row(mem_block_ + dict_[word_id].offset_,
				dict_[word_id].is_dense_,
				dict_[word_id].capacity_,
				0,
				external_buf);
			return row;
		}
		inline hybrid_alias_map get_alias_row(int word_id)
		{
			hybrid_alias_map row(alias_mem_block_ + dict_[word_id].alias_offset_,
				dict_[word_id].is_dense_,
				dict_[word_id].alias_capacity_);
			return row;
		}
		void Read(const std::string &meta_name);
	private:

		LDAModelBlock(const LDAModelBlock &other) = delete;
		LDAModelBlock& operator=(const LDAModelBlock &other) = delete;

	private:
		int32_t num_vocabs_;
		WordEntry *dict_;
		int32_t *mem_block_;
		int64_t mem_block_size_;

		int32_t *alias_mem_block_;
		int64_t alias_mem_block_size_;
	};


}