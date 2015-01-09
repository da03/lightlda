#include "alias_multinomial_rng.hpp"
#include "rand_int_rng.h"
#include <ctime>
#include <list>
#include <algorithm>

namespace wood
{
	AliasMultinomialRNG::AliasMultinomialRNG()
		: init_(false), n_(-1), mass_(0.)
	{

	}
	AliasMultinomialRNG::~AliasMultinomialRNG()
	{
	}

	void AliasMultinomialRNG::SetProportion(const std::vector<real_t> &proportion)
	{
		proportion_ = proportion;
		n_ = proportion.size();
		mass_ = 0;
		for (auto &val : proportion_)
		{
			mass_ += val;
		}
		for (auto &val : proportion_)
		{
			val /= mass_;
		}
		a_ = 1.0 / n_;

		GenerateAliasTable();
		init_ = true;
	}
	void AliasMultinomialRNG::SetProportionMass(const std::vector<real_t> &proportion, real_t mass)
	{
		proportion_ = proportion;
		n_ = proportion.size();
		mass_ = mass;

		for (auto &val : proportion_)
		{
			val /= mass_;
		}
		a_ = 1.0 / n_;

		GenerateAliasTable();
		init_ = true;
	}

	void AliasMultinomialRNG::GenerateAliasTable()
	{
		K_.resize(n_);
		V_.resize(n_);

		for (int i = 0; i < n_; ++i)
		{
			K_[i] = i;
			V_[i] = (i + 1) * a_;
		}

		int32_t L_head = 0;
		int32_t L_tail = 0;

		int32_t H_head = 0;
		int32_t H_tail = 0;

		for (auto i = 0; i < proportion_.size(); ++i)
		{
			auto val = proportion_[i];
			if (val * n_ < 1)
			{
				L_[L_tail].first = i;
				L_[L_tail].second = val;
				++L_tail;
			}
			else
			{
				H_[H_tail].first = i;
				H_[H_tail].second = val;
				++H_tail;
			}
		}

		CHECK(L_tail + H_tail == n_);

		while (L_head != L_tail && H_head != H_tail)
		{
			auto &i_pi = L_[L_head++];
			auto &h_ph = H_[H_head++];

			K_[i_pi.first] = h_ph.first;
			V_[i_pi.first] = i_pi.first * a_ + i_pi.second;

			auto sum = h_ph.second + i_pi.second;
			if (sum > 2. * a_)
			{
				H_[H_tail].first = h_ph.first;
				H_[H_tail].second = sum - a_;
				++H_tail;
			}
			else
			{
				L_[L_tail].first = h_ph.first;
				L_[L_tail].second = sum - a_;
			}
		}
		while (L_head != L_tail)
		{
			auto first = L_[L_head].first;
			auto second = L_[L_head].second;
			K_[first] = first;
			V_[first] = first  * a_ + second;
			++L_head;
		}
		while (H_head != H_tail)
		{
			auto first = H_[H_head].first;
			auto second = H_[H_head].second;
			K_[first] = first;
			V_[first] = first * a_ + second;
			++H_head;
		}
	}
	
	int32_t AliasMultinomialRNG::Next()
	{
		// NOTE(jiyuan): stl uniform_real_distribution generates the highest quality random numbers
		// yet, the other two are much faster
		//auto sample = double_distribution(engine_);

		// for double
		//auto sample = xorshift_rand()   * 4.656612875e-10;
		
		// for float
		auto sample = xorshift_rand() * 4.6566128e-10;
		
		//auto sample = intel_fast_rand() * 3.051850947e-5;
		
		// NOTE(jiyuan): use std::floor is too slow
		// here we guarantee sample * n_ is nonnegative, this makes cast work
		//int idx = std::floor(sample * n_);
		int idx = sample * n_;

		// the following code is equivalent to 
		// return sample < V_[idx] ? idx : K_[idx];
		// but faster, see
	    // http://stackoverflow.com/questions/6754454/speed-difference-between-if-else-and-ternary-operator-in-c
		int m = -(sample < V_[idx]);
		return (idx & m) | (K_[idx] & ~m);

		// int m = -(condition)
		// int a = (b & m) | (c & ~m);
		// is equivalent to a = condition ? b : c
		CHECK(false);
	}
}