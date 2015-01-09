// author: jiyuan
// date  : 2014.8.1
#pragma once



namespace wood
{
	// https://software.intel.com/en-us/articles/fast-random-number-generator-on-the-intel-pentiumr-4-processor/
	// to get uniform float in [0,1)
	// please multiply the result with 3.051850947e-5 (i.e., 1.0 / 0x7fff)

	inline int intel_fast_rand()
	{
		// the seed can be set arbitrarily
		static unsigned int g_seed = 521288629;
		g_seed = (214013 * g_seed + 2531011);
		return (g_seed >> 16) & 0x7FFF;
	}

	// George Marsaglia's xorshift approach
	// http://www.jstatsoft.org/v08/i14/paper
	// code can be found at:
	// http://www.jstatsoft.org/v11/i03
	// to get uniform float in [0,1)
	// please multiply the result with 4.656612875e-10 (i.e., 1.0 / 0x7fffffff)
	inline int xorshift_rand()
	{
		static unsigned int jxr = 521288629;
		//static unsigned int jxr = 12345678;
		jxr ^= (jxr << 13); jxr ^= (jxr >> 17); jxr ^= (jxr << 5);  //get random (xorshift) 32-bit integer
		return jxr & 0x7fffffff;
	}
	/*
	inline float fast_rand_double()
	{
	// return xorshift_rand() * 4.656612875e-10;
	return intel_fast_rand() * 3.051851e-5;
	}
	*/

	/*
	inline double fast_rand_double()
	{
	return intel_fast_rand() * 3.051851e-5;
	//return xorshift_rand() * 4.656612875e-10;
	}
	*/
	inline float xorshift_double()
	{
		//return xorshift_rand() * 4.656612875e-10;
		return xorshift_rand() * 4.6566125e-10;
	}


	inline float fast_rand_double()
	{
		// return xorshift_rand() * 4.656612875e-10;
		// return intel_fast_rand() * 3.051851e-5;
		return xorshift_double();
	}

	inline int xorshift_rand_k(int K)
	{
		return xorshift_double() * K;
	}

	inline int intel_fast_k(int K)
	{
		/*
		// the seed can be set arbitrarily
		static unsigned int g_seed = 521288629;
		//static unsigned int g_seed = 1;
		g_seed = (214013 * g_seed + 2531011);
		return (g_seed >> 16) & K;
		*/

		return fast_rand_double() *  K;
		//return xorshift_double() * K;
	}
}