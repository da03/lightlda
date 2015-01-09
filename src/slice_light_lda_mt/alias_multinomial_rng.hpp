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
		float v_;
	};

	class AliasMultinomialRNG
	{
	public:
		AliasMultinomialRNG();
		~AliasMultinomialRNG();

		void Init(int K)
		{
			L_.resize(K);
			H_.resize(K);
		}
		inline void SetProportionMass(std::vector<float> &proportion, float mass, int32_t* memory)
		{
			int32_t L_head = 0;
			int32_t L_tail = 0;
			int32_t H_head = 0;
			int32_t H_tail = 0;

			n_ = proportion.size();
			a_ = 1.0 / n_;
			mass_ = mass;

			for (int i = 0; i < n_; ++i)
			{
				auto val = proportion[i] / mass_;
				proportion[i] = val;
				// kv[i].k_ = i;
				// kv[i].v_ = (i + 1) * a_;
				int32_t *k_ptr = memory + 2 * i;
				float *v_ptr = reinterpret_cast<float*>(memory + 2 * i + 1);
				*k_ptr = i;
				*v_ptr = (i + 1) * a_;

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

			while (L_head != L_tail && H_head != H_tail)
			{
				auto &i_pi = L_[L_head++];
				auto &h_ph = H_[H_head++];


				// kv[i_pi.first].k_ = h_ph.first;
				// kv[i_pi.first].v_ = i_pi.first * a_ + i_pi.second;
				int32_t *k_ptr = memory + 2 * i_pi.first;
				float *v_ptr = reinterpret_cast<float*>(memory + 2 * i_pi.first + 1);
				*k_ptr = h_ph.first;
				*v_ptr = i_pi.first * a_ + i_pi.second;

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
					++H_tail;
				}
			}

			while (L_head != L_tail)
			{
				auto first = L_[L_head].first;
				auto second = L_[L_head].second;
				// kv[first].k_ = first;
				// kv[first].v_ = first  * a_ + second;

				int32_t *k_ptr = memory + 2 * first;
				float *v_ptr = reinterpret_cast<float*>(memory + 2 * first + 1);
				*k_ptr = first;
				*v_ptr = first * a_ + second;

				++L_head;
			}

			while (H_head != H_tail)
			{
				auto first = H_[H_head].first;
				auto second = H_[H_head].second;
				// kv[first].k_ = first;
				// kv[first].v_ = first * a_ + second;

				int32_t *k_ptr = memory + 2 * first;
				float *v_ptr = reinterpret_cast<float*>(memory + 2 * first + 1);
				*k_ptr = first;
				*v_ptr = first * a_ + second;
				++H_head;
			}
		}

		inline void SetProportionMass(std::vector<float> &proportion, int32_t size, float mass, int32_t* memory)
		{
			int32_t L_head = 0;
			int32_t L_tail = 0;
			int32_t H_head = 0;
			int32_t H_tail = 0;

			// n_ = proportion.size();
			n_ = size;
			a_ = 1.0 / n_;
			mass_ = mass;

			for (int i = 0; i < n_; ++i)
			{
				auto val = proportion[i] / mass_;
				proportion[i] = val;
				// kv[i].k_ = i;
				// kv[i].v_ = (i + 1) * a_;
				int32_t *k_ptr = memory + 2 * i;
				float *v_ptr = reinterpret_cast<float*>(memory + 2 * i + 1);
				*k_ptr = i;
				*v_ptr = (i + 1) * a_;

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

			while (L_head != L_tail && H_head != H_tail)
			{
				auto &i_pi = L_[L_head++];
				auto &h_ph = H_[H_head++];


				// kv[i_pi.first].k_ = h_ph.first;
				// kv[i_pi.first].v_ = i_pi.first * a_ + i_pi.second;
				int32_t *k_ptr = memory + 2 * i_pi.first;
				float *v_ptr = reinterpret_cast<float*>(memory + 2 * i_pi.first + 1);
				*k_ptr = h_ph.first;
				*v_ptr = i_pi.first * a_ + i_pi.second;

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
					++L_tail;
				}
			}

			while (L_head != L_tail)
			{
				auto first = L_[L_head].first;
				auto second = L_[L_head].second;
				// kv[first].k_ = first;
				// kv[first].v_ = first  * a_ + second;

				int32_t *k_ptr = memory + 2 * first;
				float *v_ptr = reinterpret_cast<float*>(memory + 2 * first + 1);
				*k_ptr = first;
				*v_ptr = first * a_ + second;

				++L_head;
			}


			while (H_head != H_tail)
			{
				auto first = H_[H_head].first;
				auto second = H_[H_head].second;
				// kv[first].k_ = first;
				// kv[first].v_ = first * a_ + second;

				int32_t *k_ptr = memory + 2 * first;
				float *v_ptr = reinterpret_cast<float*>(memory + 2 * first + 1);
				*k_ptr = first;
				*v_ptr = first * a_ + second;
				++H_head;
			}
		}

		inline void SetProportionMass(std::vector<float> &proportion, float mass, std::vector<alias_k_v>& kv)
		{
			int32_t L_head = 0;
			int32_t L_tail = 0;
			int32_t H_head = 0;
			int32_t H_tail = 0;

			n_ = proportion.size();
			a_ = 1.0 / n_;
			mass_ = mass;

			for (int i = 0; i < n_; ++i)
			{
				auto val = proportion[i] / mass_;
				proportion[i] = val;
				kv[i].k_ = i;
				kv[i].v_ = (i + 1) * a_;

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

			while (L_head != L_tail)
			{
				auto &i_pi = L_[L_head++];
				auto &h_ph = H_[H_head++];

				kv[i_pi.first].k_ = h_ph.first;
				kv[i_pi.first].v_ = i_pi.first * a_ + i_pi.second;

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
					++L_tail;
				}
			}

			while (L_head != L_tail)
			{
				auto first = L_[L_head].first;
				auto second = L_[L_head].second;
				kv[first].k_ = first;
				kv[first].v_ = first  * a_ + second;
				++L_head;
			}

			while (H_head != H_tail)
			{
				auto first = H_[H_head].first;
				auto second = H_[H_head].second;
				kv[first].k_ = first;
				kv[first].v_ = first * a_ + second;
				++H_head;
			}
		}

		// Make sure to call SetProportion or SetProportionMass before calling Next
		// int32_t Next(xorshift_rng& rng, std::vector<int32_t>& K, std::vector<float>& V);

	private:
		void GenerateAliasTable(std::vector<float> &proportion, std::vector<alias_k_v>& kv);

	public:
		AliasMultinomialRNG(const AliasMultinomialRNG &other) = delete;
		AliasMultinomialRNG& operator=(const AliasMultinomialRNG &other) = delete;

		int32_t n_;
		float a_;

		float mass_;
		bool init_;

		std::vector<std::pair<int32_t, float>> L_;
		std::vector<std::pair<int32_t, float>> H_;
	};
}