/*-------------------------------------------------------------------------
 *
 * bitmapset.h
 *	  PostgreSQL generic bitmap set package
 *
 * A bitmap set can represent any set of nonnegative integers, although
 * it is mainly intended for sets where the maximum value is not large,
 * say at most a few hundred.  By convention, a NULL pointer is always
 * accepted by all operations to represent the empty set.  (But beware
 * that this is not the only representation of the empty set.  Use
 * bms_is_empty() in preference to testing for NULL.)
 *
 *
 * Copyright (c) 2003-2017, PostgreSQL Global Development PGGroup
 *
 * src/include/nodes/bitmapset.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

#include <cstdint>

/*
 * Forward decl to save including pg_list.h
 */
struct PGList;

/*
 * Data representation
 */

/* The unit size can be adjusted by changing these three declarations: */
#define BITS_PER_BITMAPWORD 32
typedef uint32_t bitmapword;		/* must be an unsigned type */
typedef int32_t signedbitmapword; /* must be the matching signed type */

typedef struct PGBitmapset
{
	int			nwords;			/* number of words in array */
	bitmapword	words[1];	/* really [nwords] */
} PGBitmapset;


/* result of bms_subset_compare */
typedef enum PG_BMS_Comparison {
	PG_BMS_EQUAL,					/* sets are equal */
	PG_BMS_SUBSET1,				/* first set is a subset of the second */
	PG_BMS_SUBSET2,				/* second set is a subset of the first */
	BMS_DIFFERENT				/* neither set is a subset of the other */
} PG_BMS_Comparison;

/* result of bms_membership */
typedef enum PG_BMS_Membership {
	PG_BMS_EMPTY_SET,				/* 0 members */
	PG_BMS_SINGLETON,				/* 1 member */
	BMS_MULTIPLE				/* >1 member */
} PG_BMS_Membership;


/*
 * function prototypes in nodes/bitmapset.c
 */

extern PGBitmapset *bms_copy(const PGBitmapset *a);
extern bool bms_equal(const PGBitmapset *a, const PGBitmapset *b);
extern PGBitmapset *bms_make_singleton(int x);
extern void bms_free(PGBitmapset *a);

extern PGBitmapset *bms_union(const PGBitmapset *a, const PGBitmapset *b);
extern PGBitmapset *bms_intersect(const PGBitmapset *a, const PGBitmapset *b);
extern PGBitmapset *bms_difference(const PGBitmapset *a, const PGBitmapset *b);
extern bool bms_is_subset(const PGBitmapset *a, const PGBitmapset *b);
extern PG_BMS_Comparison bms_subset_compare(const PGBitmapset *a, const PGBitmapset *b);
extern bool bms_is_member(int x, const PGBitmapset *a);
extern bool bms_overlap(const PGBitmapset *a, const PGBitmapset *b);
extern bool bms_overlap_list(const PGBitmapset *a, const struct PGList *b);
extern bool bms_nonempty_difference(const PGBitmapset *a, const PGBitmapset *b);
extern int	bms_singleton_member(const PGBitmapset *a);
extern bool bms_get_singleton_member(const PGBitmapset *a, int *member);
extern int	bms_num_members(const PGBitmapset *a);

/* optimized tests when we don't need to know exact membership count: */
extern PG_BMS_Membership bms_membership(const PGBitmapset *a);
extern bool bms_is_empty(const PGBitmapset *a);

/* these routines recycle (modify or free) their non-const inputs: */

extern PGBitmapset *bms_add_member(PGBitmapset *a, int x);
extern PGBitmapset *bms_del_member(PGBitmapset *a, int x);
extern PGBitmapset *bms_add_members(PGBitmapset *a, const PGBitmapset *b);
extern PGBitmapset *bms_int_members(PGBitmapset *a, const PGBitmapset *b);
extern PGBitmapset *bms_del_members(PGBitmapset *a, const PGBitmapset *b);
extern PGBitmapset *bms_join(PGBitmapset *a, PGBitmapset *b);

/* support for iterating through the integer elements of a set: */
extern int	bms_first_member(PGBitmapset *a);
extern int	bms_next_member(const PGBitmapset *a, int prevbit);

/* support for hashtables using Bitmapsets as keys: */
extern uint32_t bms_hash_value(const PGBitmapset *a);

