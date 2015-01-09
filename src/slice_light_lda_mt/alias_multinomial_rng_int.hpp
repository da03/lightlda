// author: jiyuan
// date  : 2014.8.1
#pragma once

#include <stdint.h>
#include <random>
#include <vector>
#include <queue>
#include <memory>
#include <glog/logging.h>
#include "rand_int_rng.h"

/*
Algorithm described in 
http://www.jstatsoft.org/v11/i03/paper
George Marsaglia
Fast generation of discrete random variables
*/
namespace wood
{
	typedef float real_t;

	struct alias_k_v
	{
		int32_t k_;
		int32_t v_;
	};

	class AliasMultinomialRNGInt
	{
	public:
		AliasMultinomialRNGInt();
		~AliasMultinomialRNGInt();

		void Init(int K)
		{
			L_.resize(K);
			H_.resize(K);
			proportion_int_.resize(K);
			internal_memory_ = new int32_t[2 * K];
		}

		void SetProportionMass(std::vector<real_t> &proportion,
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

				alias_kv[first].k_ = first;
				alias_kv[first].v_ = first  * a_int_ + second;
				++L_head;
			}
			while (H_head != H_tail)
			{
				auto first = H_[H_head].first;
				auto second = H_[H_head].second;

				alias_kv[first].k_ = first;
				alias_kv[first].v_ = first * a_int_ + second;
				++H_head;
			}

		}

		inline void SetProportionMass(std::vector<float> &proportion, float mass, int32_t* memory, int32_t *height)
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
				int32_t *p = internal_memory_ + 2 * i;
				*p = i;  p++;
				*p = (i + 1) * a_int_;
			}
			
			int32_t L_head = 0;
			int32_t L_tail = 0;

			int32_t H_head = 0;
			int32_t H_tail = 0;

			for (auto i = 0; i < n_; ++i)
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

				int32_t *p = internal_memory_ + 2 * i_pi.first;
				*p = h_ph.first; p++;
				*p = i_pi.first * a_int_ + i_pi.second;

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

				int32_t *p = internal_memory_ + 2 * first;
				*p = first; p++;
				*p = first * a_int_ + second;

				++L_head;
			}
			while (H_head != H_tail)
			{
				auto first = H_[H_head].first;
				auto second = H_[H_head].second;

				int32_t *p = internal_memory_ + 2 * first;
				*p = first; p++;
				*p = first * a_int_ + second;

				++H_head;
			}	
			memcpy(memory, internal_memory_, sizeof(int32_t)* 2 * n_);
		}

		inline void SetProportionMass(std::vector<float> &proportion, int32_t size, float mass, int32_t* memory, int32_t *height)
		{
			n_ = size; // proportion.size();

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

			int32_t mass_sum = 0;
			for (int i = 0; i < n_; ++i)
			{
				proportion[i] /= mass;
				proportion_int_[i] = proportion[i] * mass_int_;
				mass_sum += proportion_int_[i];
			}

			proportion_int_[n_ - 1] += mass_int_ - mass_sum;

			int32_t L_head = 0;
			int32_t L_tail = 0;
			int32_t H_head = 0;
			int32_t H_tail = 0;

			for (int i = 0; i < n_; ++i)
			{
				int32_t *p = memory + 2 * i;
				*p = i; p++;
				*p = (i + 1) * a_int_;
			}

			for (auto i = 0; i < n_; ++i)
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

				int32_t *p = memory + 2 * i_pi.first;
				*p = h_ph.first; p++;
				*p = i_pi.first * a_int_ + i_pi.second;

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
				int32_t *p = memory + 2 * first;
				*p = first;  p++;
				*p = first * a_int_ + second;
				++L_head;
			}
			while (H_head != H_tail)
			{
				auto first = H_[H_head].first;
				auto second = H_[H_head].second;
				int32_t *p = memory + 2 * first;

				*p = first; p++;
				*p = first * a_int_ + second;
				++H_head;
			}
		}

		// Make sure to call SetProportion or SetProportionMass before calling Next
		int32_t Next(xorshift_rng& rng, std::vector<alias_k_v>& alias_kv);

	private:
		void GenerateAliasTable(std::vector<alias_k_v>& alias_kv);

	public:
		AliasMultinomialRNGInt(const AliasMultinomialRNGInt &other) = delete;
		AliasMultinomialRNGInt& operator=(const AliasMultinomialRNGInt &other) = delete;

		std::vector<int32_t> proportion_int_;
		int32_t *internal_memory_;

		int32_t n_;
		int32_t a_int_;
		int32_t mass_int_;

		bool init_;

		std::vector<std::pair<int32_t, int32_t>> L_;
		std::vector<std::pair<int32_t, int32_t>> H_;
	};
}