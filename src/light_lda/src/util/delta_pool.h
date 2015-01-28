// Author: Gao Fei(v-feigao@microsoft.com)
// Data: 2014-10-13

#pragma once

#include "util/delta_table.h"
#include "util/mt_queue_move.h"

namespace petuum {
	template <typename Delta>
	class DeltaPool {
	public:
		DeltaPool(){}
		~DeltaPool(){}
		void Init(int32_t capacity = 1024);

		void Allocate(std::unique_ptr<Delta>& delta_array);

		void Free(std::unique_ptr<Delta>& delta_array);
	private:
		void AllocateNew();
	private:
		util::MtQueueMove<std::unique_ptr<Delta>> delta_pool_;

		DeltaPool(const DeltaPool&);
		void operator=(const DeltaPool&);
	};

	template <typename Delta>
	inline void DeltaPool<Delta>::Allocate(
		std::unique_ptr<Delta>& delta_array) {
        //LOG(INFO)<<"delta_pool starts";
		delta_pool_.Pop(delta_array);
        //LOG(INFO)<<"delta_pool ends";
		return;
	}

	template <typename Delta>
	inline void DeltaPool<Delta>::Free(std::unique_ptr<Delta>& delta_array) {
		delta_array->Clear();
		delta_pool_.Push(delta_array);
	}

	template <typename Delta>
	void DeltaPool<Delta>::Init(int32_t capacity) {
		for (int32_t i = 0; i < capacity; ++i) {
			AllocateNew();
		}
	}

	template <typename Delta>
	void DeltaPool<Delta>::AllocateNew() {
		std::unique_ptr<Delta> delta_array;
		try {
			delta_array.reset(new Delta);
		}
		catch (std::bad_alloc& ba) {
			LOG(FATAL) << "Bad Alloc caught: " << ba.what();
		}
		//delta_pool_.Push(std::move(delta_array));
		delta_pool_.Push(delta_array);
	}
}
