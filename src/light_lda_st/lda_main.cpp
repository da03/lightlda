// Author: Dai Wei (wdai@cs.cmu.edu)
// Date: 2014.03.25

// NOTE(jiyuan):
// Modified from Dai Wei's Petuum LDA

#include "lda_engine.hpp"
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <thread>
#include <vector>
#include "utils.hpp"


// LDA Parameters
DEFINE_string(doc_file, "", "File containing document in LibSVM format. Each document is a line.");
DEFINE_int32(num_vocabs, -1, "Number of vocabs.");
DEFINE_double(alpha_sum, 500, "Dirichlet prior on document-topic vectors.");
DEFINE_double(beta, 0.01, "Dirichlet prior on vocab-topic vectors.");
DEFINE_int32(num_topics, 100, "Number of topics.");
DEFINE_int32(num_iterations, 10, "Number of iterations");
DEFINE_int32(compute_ll_interval, -1, "Copmute log likelihood over local dataset on every N iterations");
DEFINE_string(output_prefix, "", "output (word, topic) model");
DEFINE_int32(dump_iter, 10, "dump model every 20 iterations");

DEFINE_int32(decay_step, 100, "decay step for alpha_sum");

DEFINE_int32(gs_type, 0, "0: opfs, 1: opos");
DEFINE_int32(mh_step, 1, "number of Metropolis Hastings step");

int main(int argc, char *argv[]) {
	google::ParseCommandLineFlags(&argc, &argv, true);
	google::InitGoogleLogging(argv[0]);

	LOG(INFO) << "doc_file = " << FLAGS_doc_file;

	switch (FLAGS_gs_type)
	{
	case 0:
		LOG(INFO) << "Gibbs sampler: old proposal, fresh sampling";
		break;
	case 1:
		LOG(INFO) << "Gibbs sampler: old proposal, old sampling";
		break;
	}

	LOG(INFO) << "alpha sum = " << FLAGS_alpha_sum;
	LOG(INFO) << "beta = " << FLAGS_beta;
	LOG(INFO) << "num_topics = " << FLAGS_num_topics;
	LOG(INFO) << "num_vocabs = " << FLAGS_num_vocabs;

	// Read in data first to get # of vocabs in this partition.
	lda::LDAEngine lda_engine;

	//util::HighResolutionTimer reading_timer;
	double read_start = lda::get_time();
	LOG(INFO) << "Start reading data " << FLAGS_doc_file;
	int32_t num_partition_vocabs = lda_engine.ReadData(FLAGS_doc_file);
	double read_end = lda::get_time();
	LOG(INFO) << "Finished reading data in " << read_end - read_start << " seconds.";

	// Start LDA
	LOG(INFO) << "Starting LDA";
	// lda_engine.Start();
	lda_engine.Start();
	LOG(INFO) << "LDA finished!";

	return 0;
}
