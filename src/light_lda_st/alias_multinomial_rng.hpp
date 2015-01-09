// author: jiyuan
// date  : 2014.8.1
#pragma once

#include <stdint.h>
#include <random>
#include <vector>
#include <queue>
#include <memory>
#include <glog/logging.h>

/*
Algorithm described in 
http://www.jstatsoft.org/v11/i03/paper
George Marsaglia
Fast generation of discrete random variables
*/
namespace wood
{
	typedef float real_t;

	class AliasMultinomialRNG
	{
	public:
		//AliasMultinomialRNG();
		AliasMultinomialRNG();
		~AliasMultinomialRNG();

		void Init(int K)
		{
			L_.resize(K);
			H_.resize(K);
		}
		void SetProportion(const std::vector<real_t> &protportion);
		void SetProportionMass(const std::vector<real_t> &proportion, real_t mass);

		// Make sure to call SetProportion or SetProportionMass before calling Next
		int32_t Next();

	private:
		void GenerateAliasTable();

	public:
		AliasMultinomialRNG(const AliasMultinomialRNG &other) = delete;
		AliasMultinomialRNG& operator=(const AliasMultinomialRNG &other) = delete;

		std::vector<real_t> proportion_;
		int32_t n_;
		double a_;
		std::vector<int32_t> K_;
		std::vector<double> V_;
		double mass_;
		bool init_;

		std::vector<std::pair<int32_t, real_t>> L_;
		std::vector<std::pair<int32_t, real_t>> H_;
	};
}