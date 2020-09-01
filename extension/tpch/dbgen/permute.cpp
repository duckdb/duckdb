/*
 * permute.c -- a permutation generator for the query
 *              sequences in TPC-H and TPC-R
 */

#include "config.h"
#include "dss.h"

DSS_HUGE NextRand(DSS_HUGE seed);
void permute(long *set, int cnt, long stream);
long seed;
char *eol[2] = {" ", "},"};
static seed_t *Seed = DBGenGlobals::Seed;

#define MAX_QUERY 22
#define ITERATIONS 1000
#define UNSET 0

void permute(long *a, int c, long s) {
	int i;
	static DSS_HUGE source;
	static long temp;

	if (a != (long *)NULL) {
		for (i = 0; i < c; i++) {
			RANDOM(source, (long)i, (long)(c - 1), s);
			temp = *(a + source);
			*(a + source) = *(a + i);
			*(a + i) = temp;
		}
	}

	return;
}
