#include "random_seed.hpp"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef HAVE_BSD_STDLIB_H
#include <bsd/stdlib.h>
#endif

namespace json {

#define DEBUG_SEED(s)

/* get_time_seed */

#ifndef HAVE_ARC4RANDOM
#include <time.h>

static int get_time_seed(void) {
	DEBUG_SEED("get_time_seed");

	return (unsigned)time(NULL) * 433494437;
}
#endif

/* json_c_get_random_seed */

int json_c_get_random_seed(void) {
#ifdef OVERRIDE_GET_RANDOM_SEED
	OVERRIDE_GET_RANDOM_SEED;
#endif
#if defined HAVE_RDRAND && HAVE_RDRAND
	if (has_rdrand())
		return get_rdrand_seed();
#endif
#ifdef HAVE_ARC4RANDOM
	/* arc4random never fails, so use it if it's available */
	return arc4random();
#else
#ifdef HAVE_GETRANDOM
	{
		int seed = 0;
		if (get_getrandom_seed(&seed) == 0)
			return seed;
	}
#endif
#if defined HAVE_DEV_RANDOM && HAVE_DEV_RANDOM
	{
		int seed = 0;
		if (get_dev_random_seed(&seed) == 0)
			return seed;
	}
#endif
#if defined HAVE_CRYPTGENRANDOM && HAVE_CRYPTGENRANDOM
	{
		int seed = 0;
		if (get_cryptgenrandom_seed(&seed) == 0)
			return seed;
	}
#endif
	return get_time_seed();
#endif /* !HAVE_ARC4RANDOM */
}

} // namespace json
