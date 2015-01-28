#pragma once

#include <vector>
#include "util/delta_table.h"
#include "util/delta_pool.h"
#include "util/mt_queue_move.h"


namespace lda {
	// DeltaCraft handle all concurrence issues between several app threads (as Producer) 
	// and several delta threads (as Consumer). 
	// It is in spirit similar with MapReduce paradigm. 
	
	class DeltaCraft {
		typedef util::MtQueueMove<petuum::DeltaArray> DeltaQueue;
	public:
		void Push(int32_t thread_id);
		void Pop(int32_t thread_id);
		
	private:
		int32_t num_app_threads_;
		int32_t num_delta_threads_;
		
		std::vector<std::unique_ptr<DeltaQueue>> delta_queue_vec_;
		
		petuum::DeltaPool<petuum::DeltaArray> delta_pool_;
	};
}