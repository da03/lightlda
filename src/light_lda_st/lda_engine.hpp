// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2014.03.29

#pragma once

#include <memory>
#include "lda_document.hpp"
#include "light_doc_sampler.hpp"
#include "context.hpp"
#include <cstdint>
#include <utility>
#include <vector>
#include <string>
#include <set>
#include <atomic>

namespace lda {

	// Engine takes care of the entire pipeline of LDA, from reading data to
	// spawning threads, to recording execution time and loglikelihood.
	class LDAEngine {
	public:
		LDAEngine();

		// Read libSVM formatted document (.dat) data. This should only be called
		// once by one thread.  Returning # of vocabs.
		int32_t ReadData(const std::string& doc_file);

		void Start();
		void Dump(int iter);

	private:  // private functions
		void AddWordTopics(Document* doc, int32_t word, int32_t num_tokens);

		// Get a document from docs_.
		//inline LDACorpus::iterator GetOneDoc();

	private:  // private data

		int32_t K_;   // number of topics

		// Number of vocabs.
		int32_t V_;

		// vocabs in this data partition.
		std::set<int32_t> local_vocabs_;

		// Compute complete log-likelihood (ll) every compute_ll_interval_
		// iterations.
		int32_t compute_ll_interval_;

		DocBatch docs_;

		// # of tokens processed in a Clock() call.
		std::atomic<int32_t> num_tokens_clock_;

		LightDocSampler sampler_;
	};

}   // namespace lda
