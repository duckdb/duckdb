/*
 * Copyright owned by the Transaction Processing Performance Council.
 *
 * A copy of the license is included under extension/tpch/dbgen/LICENSE
 * in this repository.
 *
 * You may not use this file except in compliance with the License.
 *
 * THE TPC SOFTWARE IS AVAILABLE WITHOUT CHARGE FROM TPC.
 */
/*
 * text.c --- pseaudo text generator for use in DBGEN 2.0
 *
 * Defined Routines:
 *		dbg_text() -- select and translate a sentance form
 */

#ifdef TEXT_TEST
#define DECLARER
#endif /* TEST */

#include "dbgen/config.h"

#include <stdlib.h>
#ifndef WIN32
 /* Change for Windows NT */
#include <unistd.h>
#endif /* WIN32 */
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <math.h>
#include <signal.h>
#include <stdio.h> /* */
#include <string.h>
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

#include "dbgen/dss.h"
#include "dbgen/dsstypes.h"

/*
 * txt_vp() --
 *		generate a verb phrase by
 *		1) selecting a verb phrase form
 *		2) parsing it to select parts of speech
 *		3) selecting appropriate words
 *		4) adding punctuation as required
 *
 *	Returns: length of generated phrase
 *	Called By: txt_sentence()
 *	Calls: pick_str()
 */
static int txt_vp(char *dest, seed_t *seed) {
	char syntax[MAX_GRAMMAR_LEN + 1], *cptr, *parse_target;
	distribution *src;
	int i, res = 0;

	pick_str(&vp, seed, &syntax[0]);
	parse_target = syntax;
	while ((cptr = strtok(parse_target, " ")) != NULL) {
		src = NULL;
		switch (*cptr) {
		case 'D':
			src = &adverbs;
			break;
		case 'V':
			src = &verbs;
			break;
		case 'X':
			src = &auxillaries;
			break;
		} /* end of POS switch statement */
		i = pick_str(src, seed, dest);
		i = (int)strlen(DIST_MEMBER(src, i));
		dest += i;
		res += i;
		if (*(++cptr)) /* miscelaneous fillagree, like punctuation */
		{
			dest += 1;
			res += 1;
			*dest = *cptr;
		}
		*dest = ' ';
		dest++;
		res++;
		parse_target = NULL;
	} /* end of while loop */

	return (res);
}

/*
 * txt_np() --
 *		generate a noun phrase by
 *		1) selecting a noun phrase form
 *		2) parsing it to select parts of speech
 *		3) selecting appropriate words
 *		4) adding punctuation as required
 *
 *	Returns: length of generated phrase
 *	Called By: txt_sentence()
 *	Calls: pick_str(),
 */
static int txt_np(char *dest, seed_t *seed) {
	char syntax[MAX_GRAMMAR_LEN + 1], *cptr, *parse_target;
	distribution *src;
	int i, res = 0;

	pick_str(&np, seed, &syntax[0]);
	parse_target = syntax;
	while ((cptr = strtok(parse_target, " ")) != NULL) {
		src = NULL;
		switch (*cptr) {
		case 'A':
			src = &articles;
			break;
		case 'J':
			src = &adjectives;
			break;
		case 'D':
			src = &adverbs;
			break;
		case 'N':
			src = &nouns;
			break;
		} /* end of POS switch statement */
		i = pick_str(src, seed, dest);
		i = (int)strlen(DIST_MEMBER(src, i));
		dest += i;
		res += i;
		if (*(++cptr)) /* miscelaneous fillagree, like punctuation */
		{
			*dest = *cptr;
			dest += 1;
			res += 1;
		}
		*dest = ' ';
		dest++;
		res++;
		parse_target = NULL;
	} /* end of while loop */

	return (res);
}

/*
 * txt_sentence() --
 *		generate a sentence by
 *		1) selecting a sentence form
 *		2) parsing it to select parts of speech or phrase types
 *		3) selecting appropriate words
 *		4) adding punctuation as required
 *
 *	Returns: length of generated sentence
 *	Called By: dbg_text()
 *	Calls: pick_str(), txt_np(), txt_vp()
 */
static int txt_sentence(char *dest, seed_t *seed) {
	char syntax[MAX_GRAMMAR_LEN + 1], *cptr;
	int i, res = 0, len = 0;

	pick_str(&grammar, seed, syntax);
	cptr = syntax;

next_token: /* I hate goto's, but can't seem to have parent and child use strtok() */
	while (*cptr && *cptr == ' ')
		cptr++;
	if (*cptr == '\0')
		goto done;
	switch (*cptr) {
	case 'V':
		len = txt_vp(dest, seed);
		break;
	case 'N':
		len = txt_np(dest, seed);
		break;
	case 'P':
		i = pick_str(&prepositions, seed, dest);
		len = (int)strlen(DIST_MEMBER(&prepositions, i));
		strcpy((dest + len), " the ");
		len += 5;
		len += txt_np(dest + len, seed);
		break;
	case 'T':
		i = pick_str(&terminators, seed, --dest); /*terminators should abut previous word */
		len = (int)strlen(DIST_MEMBER(&terminators, i));
		break;
	} /* end of POS switch statement */
	dest += len;
	res += len;
	cptr++;
	if (*cptr && *cptr != ' ') /* miscelaneous fillagree, like punctuation */
	{
		dest += 1;
		res += 1;
		*dest = *cptr;
	}
	goto next_token;
done:
	*dest = '\0';
	return (--res);
}

static char *gen_text(char *dest, seed_t *seed, distribution *s) {
	long i = 0;
	DSS_HUGE j;

	RANDOM(j, 1, s->list[s->count - 1].weight, seed);
	while (s->list[i].weight < j)
		i++;
	char *src = s->list[i].text;
	int ind = 0;
	while (src[ind]) {
		dest[ind] = src[ind];
		ind++;
	}
	dest[ind] = ' ';
	return dest + ind + 1;
}

#define NOUN_MAX_WEIGHT 340
#define ADJECTIVES_MAX_WEIGHT 289
#define ADVERBS_MAX_WEIGHT 262
#define AUXILLARIES_MAX_WEIGHT 18
#define VERBS_MAX_WEIGHT 174
#define PREPOSITIONS_MAX_WEIGHT 456

static char *noun_index[NOUN_MAX_WEIGHT + 1];
static char *adjectives_index[ADJECTIVES_MAX_WEIGHT + 1];
static char *adverbs_index[ADVERBS_MAX_WEIGHT + 1];
static char *auxillaries_index[AUXILLARIES_MAX_WEIGHT + 1];
static char *verbs_index[VERBS_MAX_WEIGHT + 1];
static char *prepositions_index[PREPOSITIONS_MAX_WEIGHT + 1];

static char *szTextPool = NULL;
static long txtBufferSize = 0;

// generate a lookup table for weight -> str
static void gen_index(char **index, distribution *s) {
	for (size_t w = 0; w <= s->list[s->count - 1].weight; w++) {
		long i = 0;
		while (s->list[i].weight < w)
			i++;
		index[w] = s->list[i].text;
	}
}

static char *gen_text_index(char *dest, seed_t *seed, char **index, distribution *s) {
	long i = 0;
	DSS_HUGE j;

	RANDOM(j, 1, s->list[s->count - 1].weight, seed);
	char *src = index[j];
	int ind = 0;
	while (src[ind]) {
		dest[ind] = src[ind];
		ind++;
	}
	dest[ind] = ' ';
	return dest + ind + 1;
}

static char *gen_vp(char *dest, seed_t *seed) {
	DSS_HUGE j;
	RANDOM(j, 1, vp.list[vp.count - 1].weight, seed);
	int index = 0;
	index += vp.list[0].weight < j;
	index += vp.list[1].weight < j;
	index += vp.list[2].weight < j;

	if (index == 0) {
		dest = gen_text_index(dest, seed, verbs_index, &verbs);
	} else if (index == 1) {
		dest = gen_text_index(dest, seed, auxillaries_index, &auxillaries);
		dest = gen_text_index(dest, seed, verbs_index, &verbs);
	} else if (index == 2) {
		dest = gen_text_index(dest, seed, verbs_index, &verbs);
		dest = gen_text_index(dest, seed, adverbs_index, &adverbs);
	} else {
		dest = gen_text_index(dest, seed, auxillaries_index, &auxillaries);
		dest = gen_text_index(dest, seed, verbs_index, &verbs);
		dest = gen_text_index(dest, seed, adverbs_index, &adverbs);
	}
	return dest;
}

static char *gen_np(char *dest, seed_t *seed) {
	DSS_HUGE j;
	RANDOM(j, 1, np.list[np.count - 1].weight, seed);
	int index = 0;
	index += np.list[0].weight < j;
	index += np.list[1].weight < j;
	index += np.list[2].weight < j;

	if (index == 0) {
		dest = gen_text_index(dest, seed, noun_index, &nouns);
	} else if (index == 1) {
		dest = gen_text_index(dest, seed, adjectives_index, &adjectives);
		dest = gen_text_index(dest, seed, noun_index, &nouns);
	} else if (index == 2) {
		dest = gen_text_index(dest, seed, adjectives_index, &adjectives);
		dest[-1] = ',';
		*(dest++) = ' ';
		dest = gen_text_index(dest, seed, adjectives_index, &adjectives);
		dest = gen_text_index(dest, seed, noun_index, &nouns);
	} else {
		dest = gen_text_index(dest, seed, adverbs_index, &adverbs);
		dest = gen_text_index(dest, seed, adjectives_index, &adjectives);
		dest = gen_text_index(dest, seed, noun_index, &nouns);
	}
	return dest;
}

static char *gen_preposition(char *dest, seed_t *seed) {
	dest = gen_text_index(dest, seed, prepositions_index, &prepositions);
	*(dest++) = 't';
	*(dest++) = 'h';
	*(dest++) = 'e';
	*(dest++) = ' ';
	return gen_np(dest, seed);
}

static char *gen_terminator(char *dest, seed_t *seed) {
	dest = gen_text(--dest, seed, &terminators);
	return dest - 1;
}

static char *gen_sentence(char *dest, seed_t *seed) {
	const char *cptr;
	int i;

	DSS_HUGE j;
	RANDOM(j, 1, grammar.list[grammar.count - 1].weight, seed);
	int index = 0;
	index += grammar.list[0].weight < j;
	index += grammar.list[1].weight < j;
	index += grammar.list[2].weight < j;
	index += grammar.list[3].weight < j;
	cptr = grammar.list[index].text;

	if (index == 0) {
		dest = gen_np(dest, seed);
		dest = gen_vp(dest, seed);
		dest = gen_terminator(dest, seed);
	} else if (index == 1) {
		dest = gen_np(dest, seed);
		dest = gen_vp(dest, seed);
		dest = gen_preposition(dest, seed);
		dest = gen_terminator(dest, seed);
	} else if (index == 2) {
		dest = gen_np(dest, seed);
		dest = gen_vp(dest, seed);
		dest = gen_np(dest, seed);
		dest = gen_terminator(dest, seed);
	} else if (index == 3) {
		dest = gen_np(dest, seed);
		dest = gen_preposition(dest, seed);
		dest = gen_vp(dest, seed);
		dest = gen_np(dest, seed);
		dest = gen_terminator(dest, seed);
	} else {
		dest = gen_np(dest, seed);
		dest = gen_preposition(dest, seed);
		dest = gen_vp(dest, seed);
		dest = gen_preposition(dest, seed);
		dest = gen_terminator(dest, seed);
	}
	*dest = ' ';
	return dest + 1;
}

/*
 * init_text_pool() --
 *    allocate and initialize the internal text pool buffer (szTextPool).
 *    Make sure to call it before using dbg_text().
 */
void init_text_pool(long bSize, DBGenContext *ctx) {
	gen_index(noun_index, &nouns);
	gen_index(adjectives_index, &adjectives);
	gen_index(adverbs_index, &adverbs);
	gen_index(auxillaries_index, &auxillaries);
	gen_index(verbs_index, &verbs);
	gen_index(prepositions_index, &prepositions);

  txtBufferSize = bSize;
  szTextPool = (char*)malloc(bSize + 1 + 100);

	char *ptr = szTextPool;
	char *endptr = szTextPool + bSize + 1;
	while (ptr < endptr) {
		ptr = gen_sentence(ptr, &ctx->Seed[5]);
	}
	szTextPool[bSize] = '\0';
}

void free_text_pool() {
  free(szTextPool);
}

/*
 * dbg_text() --
 *		produce ELIZA-like text of random, bounded length, truncating the last
 *		generated sentence as required
 */
void dbg_text(char *tgt, int min, int max, seed_t *seed) {
	DSS_HUGE hgLength = 0, hgOffset, wordlen = 0, s_len, needed;
	char sentence[MAX_SENT_LEN + 1], *cp;

	RANDOM(hgOffset, 0, txtBufferSize - max, seed);
	RANDOM(hgLength, min, max, seed);
	strncpy(&tgt[0], &szTextPool[hgOffset], (int)hgLength);
	tgt[hgLength] = '\0';

	return;
}
