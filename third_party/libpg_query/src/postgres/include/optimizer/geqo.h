/*-------------------------------------------------------------------------
 *
 * geqo.h
 *	  prototypes for various files in optimizer/geqo
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/geqo.h
 *
 *-------------------------------------------------------------------------
 */

/* contributed by:
   =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
   *  Martin Utesch				 * Institute of Automatic Control	   *
   =							 = University of Mining and Technology =
   *  utesch@aut.tu-freiberg.de  * Freiberg, Germany				   *
   =*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=*=
 */

#ifndef GEQO_H
#define GEQO_H

#include "nodes/relation.h"
#include "optimizer/geqo_gene.h"


/* GEQO debug flag */
/*
 #define GEQO_DEBUG
 */

/* recombination mechanism */
/*
 #define ERX
 #define PMX
 #define CX
 #define PX
 #define OX1
 #define OX2
 */
#define ERX


/*
 * Configuration options
 *
 * If you change these, update backend/utils/misc/postgresql.conf.sample
 */
extern int	Geqo_effort;		/* 1 .. 10, knob for adjustment of defaults */

#define DEFAULT_GEQO_EFFORT 5
#define MIN_GEQO_EFFORT 1
#define MAX_GEQO_EFFORT 10

extern int	Geqo_pool_size;		/* 2 .. inf, or 0 to use default */

extern int	Geqo_generations;	/* 1 .. inf, or 0 to use default */

extern double Geqo_selection_bias;

#define DEFAULT_GEQO_SELECTION_BIAS 2.0
#define MIN_GEQO_SELECTION_BIAS 1.5
#define MAX_GEQO_SELECTION_BIAS 2.0

extern double Geqo_seed;		/* 0 .. 1 */


/*
 * Private state for a GEQO run --- accessible via root->join_search_private
 */
typedef struct
{
	List	   *initial_rels;	/* the base relations we are joining */
	unsigned short random_state[3];		/* state for pg_erand48() */
} GeqoPrivateData;


/* routines in geqo_main.c */
extern RelOptInfo *geqo(PlannerInfo *root,
	 int number_of_rels, List *initial_rels);

/* routines in geqo_eval.c */
extern Cost geqo_eval(PlannerInfo *root, Gene *tour, int num_gene);
extern RelOptInfo *gimme_tree(PlannerInfo *root, Gene *tour, int num_gene);

#endif   /* GEQO_H */
