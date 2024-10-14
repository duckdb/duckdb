/* @(#)permute.c	2.1.8.3 */
/*
 * permute.c -- a permutation generator for the query
 *              sequences in TPC-H and TPC-R
 */

#ifdef TEST
#define DECLARER
#endif
#include "include/config.h"
#include "include/dss.h"
#ifdef TEST
#include <stdlib.h>
#if (defined(_POSIX_) || !defined(WIN32)) /* Change for Windows NT */
#include <unistd.h>
#include <sys/wait.h>
#endif			   /* WIN32 */
#include <stdio.h> /* */
#include <limits.h>
#include <math.h>
#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <errno.h>
#ifdef HP
#include <strings.h>
#endif
#if (defined(WIN32) && !defined(_POSIX_))
#include <process.h>
#pragma warning(disable : 4201)
#pragma warning(disable : 4214)
#pragma warning(disable : 4514)
#define WIN32_LEAN_AND_MEAN
#define NOATOM
#define NOGDICAPMASKS
#define NOMETAFILE
#define NOMINMAX
#define NOMSG
#define NOOPENFILE
#define NORASTEROPS
#define NOSCROLL
#define NOSOUND
#define NOSYSMETRICS
#define NOTEXTMETRIC
#define NOWH
#define NOCOMM
#define NOKANJI
#define NOMCX
#include <windows.h>
#pragma warning(default : 4201)
#pragma warning(default : 4214)
#endif
#endif

long NextRand(long seed);
long *permute(long *set, int cnt, long stream);
long *permute_dist(distribution *d, long stream);
long seed;
char *eol[2] = {" ", "},"};
extern seed_t Seed[];
#ifdef TEST
tdef tdefs = {NULL};
#endif

#define MAX_QUERY 22
#define ITERATIONS 1000
#define UNSET 0

long *
permute(long *a, int c, long s)
{
	int i;
	static long source;
	static long *set, temp;

	if (a != (long *)NULL)
	{
		set = a;
		for (i = 0; i < c; i++)
			*(a + i) = i;
		for (i = 0; i < c; i++)
		{
			RANDOM(source, 0L, (long)(c - 1), s);
			temp = *(a + source);
			*(a + source) = *(a + i);
			*(a + i) = temp;
			source = 0;
		}
	}
	else
		source += 1;

	if (source >= c)
		source -= c;

	return (set + source);
}

long *
permute_dist(distribution *d, long stream)
{
	static distribution *dist = NULL;
	int i;

	if (d != NULL)
	{
		if (d->permute == (long *)NULL)
		{
			d->permute = (long *)malloc(sizeof(long) * DIST_SIZE(d));
			MALLOC_CHECK(d->permute);
			for (i = 0; i < DIST_SIZE(d); i++)
				*(d->permute + i) = i;
		}
		dist = d;
		return (permute(dist->permute, DIST_SIZE(dist), stream));
	}

	if (dist != NULL)
		return (permute(NULL, DIST_SIZE(dist), stream));
	else
		INTERNAL_ERROR("Bad call to permute_dist");
}

#ifdef TEST

main(int ac, char *av[])
{
	long *sequence,
		i,
		j,
		streams = UNSET,
		*a;
	char sep;
	int index = 0;

	set_seeds = 0;
	sequence = (long *)malloc(MAX_QUERY * sizeof(long));
	a = sequence;
	for (i = 0; i < MAX_QUERY; i++)
		*(sequence + i) = i;
	if (ac < 3)
		goto usage;
	Seed[0].value = (long)atoi(av[1]);
	streams = atoi(av[2]);
	if (Seed[0].value == UNSET || streams == UNSET)
		goto usage;

	index = 0;
	printf("long permutation[%d][%d] = {\n", streams, MAX_QUERY);
	for (j = 0; j < streams; j++)
	{
		sep = '{';
		printf("%s\n", eol[index]);
		for (i = 0; i < MAX_QUERY; i++)
		{
			printf("%c%2d", sep, *permute(a, MAX_QUERY, 0) + 1);
			a = (long *)NULL;
			sep = ',';
		}
		a = sequence;
		index = 1;
	}
	printf("}\n};\n");
	return (0);

usage:
	printf("Usage: %s <seed> <streams>\n", av[0]);
	printf("  uses <seed> to start the generation of <streams> permutations of [1..%d]\n", MAX_QUERY);
	return (-1);
}
#endif /* TEST */
