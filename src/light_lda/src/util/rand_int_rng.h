// author: jiyuan
// date  : 2014.8.1
#pragma once

namespace wood
{
	class xorshift_rng
	{
	public:
		xorshift_rng()
		{
			// jxr = 1234567;
			jxr = time(NULL);
		}
		~xorshift_rng() {}

		int32_t rand()
		{
			jxr ^= (jxr << 13); jxr ^= (jxr >> 17); jxr ^= (jxr << 5);  //get random (xorshift) 32-bit integer
			return jxr & 0x7fffffff;
		}
		double rand_double()
		{
			return rand() * 4.6566125e-10;
		}
		int32_t rand_k(int K)
		{
			return rand() * 4.6566125e-10 * K;
		}
	private:
		xorshift_rng(const xorshift_rng &other) = delete;
		xorshift_rng& operator=(const xorshift_rng &other) = delete;

		unsigned int jxr;
	};
}