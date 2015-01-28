// Author: Jinhui Yuan (jiyuan@microsoft.com)
// Date: 2014.10.01

#pragma once
#include <stdint.h>
#include <fstream>
#include <hash_map>
#include <cassert>
#include <iterator>
/*
A light-weight hash table, borrowing the idea from google::dense_hash_map
0, <key, value> pair must be <int32_t, int32_t>
1, It can or can not own memory,
2, It has a fixed capacity, needless to resize or shrink,
3, capacity_ should at lease be twice of the maximum number of inserted items, guaranteeing a low load factor,
4, capacity_ should be an integer power of 2
5, emptry_key_ is fixed to -1
6, deleted_key_ is fixed to -2
*/

// TODO(jiyuan): evalute whether we need re-hashing after a lot of insertion and deletion

namespace wood
{
	// The probing method:
	// Linear probing
	//#define JUMP_(key, num_probes)    ( 1 )

	// Quadratic probing
#define JUMP_(key, num_probes)    ( num_probes )

#define ILLEGAL_BUCKET -1

	class light_hash_map
	{
	public:

		// NOTE(jiyuan): the size of mem_block_ = 2 * capacity_
		light_hash_map(int32_t *mem_block, int32_t capacity) :
			own_memory_(false),
			capacity_(capacity),
			mem_block_(mem_block),
			internal_key_(0),
			empty_key_(0),
			deleted_key_(-2)
		{
			key_ = mem_block_;
			value_ = mem_block_ + capacity_;
			clear();
		}

		light_hash_map(int32_t capacity) :
			own_memory_(true),
			capacity_(capacity),
			internal_key_(0),
			empty_key_(0),
			deleted_key_(-2)
		{
			mem_block_ = new int32_t[capacity_ * 2]();
			key_ = mem_block_;
			value_ = mem_block_ + capacity_;
			clear();
		}

		// must call set_memory after construction before use
		light_hash_map() :
			capacity_(1024),
			own_memory_(false),
			internal_key_(0),
			empty_key_(0),
			deleted_key_(-2),
			mem_block_(nullptr),
			key_(nullptr),
			value_(nullptr)
		{
		}

		~light_hash_map()
		{
			capacity_ = 0;
			if (own_memory_ && mem_block_ != nullptr)
			{
				delete[]mem_block_;
			}

			mem_block_ = nullptr;
			key_ = nullptr;
			value_ = nullptr;
		}

		inline void clear()
		{
			//std::fill(key_, key_ + capacity_, -1);
			//std::fill(value_, value_ + capacity_, 0);
			memset(mem_block_, 0, 2 * capacity_ * sizeof(int32_t));
		}

		inline void set_memory(int32_t *mem_block)
		{
			mem_block_ = mem_block;
			key_ = mem_block_;
			value_ = mem_block_ + capacity_;
		}

		inline int32_t capacity() const { return capacity_; }
		inline int32_t size() const
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

		inline int32_t* key() const {
			return key_;
		}

		inline int32_t* value() const {
			return value_;
		}

		// whether we can find the |key| in this hash table
		inline bool has(int32_t key) const
		{
			internal_key_ = key + 1;
			std::pair<int32_t, int32_t> pos = find_position(internal_key_);
			return pos.first != ILLEGAL_BUCKET;
		}

		// if |key| is already in table, increase its coresponding |value| with |delta|
		// if not, insert |key| into the table and set |delta| as the |value| of |key|
		inline void inc(int32_t key, int32_t delta)
		{
			internal_key_ = key + 1;
			std::pair<int32_t, int32_t> pos = find_position(internal_key_);
			if (pos.first != ILLEGAL_BUCKET)
			{
				value_[pos.first] += delta;
				if (value_[pos.first] == 0)       // the value becomes zero, delete the key
				{
					key_[pos.first] = deleted_key_;
					//--size_;
				}
			}
			else                                 // not found the key, insert it with delta as value
			{
				key_[pos.second] = internal_key_;
				value_[pos.second] = delta;
				//++size_;
			}
		}

		// query the value of |key|
		// if |key| is in the table, return the |value| corresonding to |key|
		// if not, just return 0
		inline int32_t operator[](int32_t key)
		{
			internal_key_ = key + 1;
			std::pair<int32_t, int32_t> pos = find_position(internal_key_);
			if (pos.first != ILLEGAL_BUCKET)
			{
				return value_[pos.first];
			}
			else
			{
				return 0;
			}
		}

	private:

		light_hash_map(const light_hash_map &other) = delete;
		light_hash_map& operator=(const light_hash_map &other) = delete;

		// Returns a pair of positions: 1st where the object is, 2nd where
		// it would go if you wanted to insert it.  1st is ILLEGAL_BUCKET
		// if object is not found; 2nd is ILLEGAL_BUCKET if it is.
		// Note: because of deletions where-to-insert is not trivial: it's the
		// first deleted bucket we see, as long as we don't find the key later
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
				assert(num_probes < capacity_
					&& "Hashtable is full: an error in key_equal<> or hash<>");
			}
		}

		bool own_memory_;
		int32_t capacity_;
		// int32_t size_;
		int32_t *mem_block_;
		int32_t *key_;
		int32_t *value_;

		mutable int32_t internal_key_;

		int32_t empty_key_;
		int32_t deleted_key_;
		//stdext::hash_compare<int32_t> hasher_;
	};
}
