#include "alias_multinomial_rng_int.hpp"
#include "rand_int_rng.h"
#include <ctime>
#include <list>
#include <algorithm>
#include <iostream>

namespace wood
{
	AliasMultinomialRNGInt::AliasMultinomialRNGInt()
		: init_(false), n_(-1)
	{

	}
	AliasMultinomialRNGInt::~AliasMultinomialRNGInt()
	{
		delete[]internal_memory_;
	}
	/*
	void AliasMultinomialRNGInt::SetProportionMass(std::vector<real_t> &proportion,
		real_t mass,
		std::vector<alias_k_v> &alias_kv, int32_t *height)
	{
		n_ = proportion.size();

		mass_int_ = 0x7fffffff;
		if (mass_int_ % n_ == 0)
		{
			a_int_ = mass_int_ / n_;
		}
		else
		{
			a_int_ = mass_int_ / n_ + 1;
		}
		*height = a_int_;

		proportion_int_.resize(n_);
		
		int32_t mass_sum = 0;
		for (int i = 0; i < n_; ++i)
		{
			proportion[i] /= mass;
			proportion_int_[i] = proportion[i] * mass_int_;
			mass_sum += proportion_int_[i];
		}

		proportion_int_[n_ - 1] += mass_int_ - mass_sum;

		for (int i = 0; i < n_; ++i)
		{
			alias_kv[i].k_ = i;
			alias_kv[i].v_ = (i + 1) * a_int_;
		}

		int32_t L_head = 0;
		int32_t L_tail = 0;

		int32_t H_head = 0;
		int32_t H_tail = 0;

		for (auto i = 0; i < proportion_int_.size(); ++i)
		{
			auto val = proportion_int_[i];
			if (val < a_int_)
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

			//K[i_pi.first] = h_ph.first;
			//V[i_pi.first] = i_pi.first * a_int_ + i_pi.second;
			alias_kv[i_pi.first].k_ = h_ph.first;
			alias_kv[i_pi.first].v_ = i_pi.first * a_int_ + i_pi.second;

			auto sum = h_ph.second + i_pi.second;
			if (sum > 2 * a_int_)
			{
				H_[H_tail].first = h_ph.first;
				H_[H_tail].second = sum - a_int_;
				++H_tail;
			}
			else
			{
				L_[L_tail].first = h_ph.first;
				L_[L_tail].second = sum - a_int_;
				++L_tail;
			}
		}
		while (L_head != L_tail)
		{
			auto first = L_[L_head].first;
			auto second = L_[L_head].second;
			//K[first] = first;
			//V[first] = first  * a_int_ + second;
			alias_kv[first].k_ = first;
			alias_kv[first].v_ = first  * a_int_ + second;
			++L_head;
		}
		while (H_head != H_tail)
		{
			auto first = H_[H_head].first;
			auto second = H_[H_head].second;
			// K[first] = first;
			// V[first] = first * a_int_ + second;
			alias_kv[first].k_ = first;
			alias_kv[first].v_ = first * a_int_ + second;
			++H_head;
		}

		// GenerateAliasTable(alias_kv);
		// init_ = true;
	}

	*/

	void AliasMultinomialRNGInt::GenerateAliasTable(std::vector<alias_k_v>& alias_kv)
	{
		//K.resize(n_);
		//V.resize(n_);

		for (int i = 0; i < n_; ++i)
		{
			alias_kv[i].k_ = i;
			alias_kv[i].v_ = (i + 1) * a_int_;
		}

		int32_t L_head = 0;
		int32_t L_tail = 0;

		int32_t H_head = 0;
		int32_t H_tail = 0;

		for (auto i = 0; i < proportion_int_.size(); ++i)
		{
			auto val = proportion_int_[i];
			if (val < a_int_)
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

			//K[i_pi.first] = h_ph.first;
			//V[i_pi.first] = i_pi.first * a_int_ + i_pi.second;
			alias_kv[i_pi.first].k_ = h_ph.first;
			alias_kv[i_pi.first].v_ = i_pi.first * a_int_ + i_pi.second;

			auto sum = h_ph.second + i_pi.second;
			if (sum > 2 * a_int_)
			{
				H_[H_tail].first = h_ph.first;
				H_[H_tail].second = sum - a_int_;
				++H_tail;
			}
			else
			{
				L_[L_tail].first = h_ph.first;
				L_[L_tail].second = sum - a_int_;
				++L_tail;
			}
		}
		while (L_head != L_tail)
		{
			auto first = L_[L_head].first;
			auto second = L_[L_head].second;
			//K[first] = first;
			//V[first] = first  * a_int_ + second;
			alias_kv[first].k_ = first;
			alias_kv[first].v_ = first  * a_int_ + second;
			++L_head;
		}
		while (H_head != H_tail)
		{
			auto first = H_[H_head].first;
			auto second = H_[H_head].second;
			// K[first] = first;
			// V[first] = first * a_int_ + second;
			alias_kv[first].k_ = first;
			alias_kv[first].v_ = first * a_int_ + second;
			++H_head;
		}
	}
	
	int32_t AliasMultinomialRNGInt::Next(xorshift_rng& rng, std::vector<alias_k_v>& alias_kv)
	{
		// NOTE(jiyuan): stl uniform_real_distribution generates the highest quality random numbers
		// yet, the other two are much faster
		//auto sample = double_distribution(engine_);

		// for double
		//auto sample = xorshift_rand()   * 4.656612875e-10;
		
		// for float
		// auto sample = rng.rand_real(); // rng.rand() * 4.6566128e-10;
		// auto sample = rng.rand_k(mass_int_);
		auto sample = rng.rand();
		
		//auto sample = intel_fast_rand() * 3.051850947e-5;
		
		// NOTE(jiyuan): use std::floor is too slow
		// here we guarantee sample * n_ is nonnegative, this makes cast work
		//int idx = std::floor(sample * n_);
		int idx = sample / a_int_;

		// CHECK(idx < K.size()) << "sample = " << sample << ",a_int_ = " << a_int_ << ",idx=" << idx;
		// the following code is equivalent to 
		// return sample < V_[idx] ? idx : K_[idx];
		// but faster, see
	    // http://stackoverflow.com/questions/6754454/speed-difference-between-if-else-and-ternary-operator-in-c
		int m = -(sample < alias_kv[idx].v_);
		return (idx & m) | (alias_kv[idx].k_ & ~m);
		// return sample < V[idx] ? idx : K[idx];

		// int m = -(condition)
		// int a = (b & m) | (c & ~m);
		// is equivalent to a = condition ? b : c
		CHECK(false);
	}
}