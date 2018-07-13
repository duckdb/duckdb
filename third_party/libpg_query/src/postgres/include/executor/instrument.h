/*-------------------------------------------------------------------------
 *
 * instrument.h
 *	  definitions for run-time statistics collection
 *
 *
 * Copyright (c) 2001-2015, PostgreSQL Global Development Group
 *
 * src/include/executor/instrument.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INSTRUMENT_H
#define INSTRUMENT_H

#include "portability/instr_time.h"


typedef struct BufferUsage
{
	long		shared_blks_hit;	/* # of shared buffer hits */
	long		shared_blks_read;		/* # of shared disk blocks read */
	long		shared_blks_dirtied;	/* # of shared blocks dirtied */
	long		shared_blks_written;	/* # of shared disk blocks written */
	long		local_blks_hit; /* # of local buffer hits */
	long		local_blks_read;	/* # of local disk blocks read */
	long		local_blks_dirtied;		/* # of shared blocks dirtied */
	long		local_blks_written;		/* # of local disk blocks written */
	long		temp_blks_read; /* # of temp blocks read */
	long		temp_blks_written;		/* # of temp blocks written */
	instr_time	blk_read_time;	/* time spent reading */
	instr_time	blk_write_time; /* time spent writing */
} BufferUsage;

/* Flag bits included in InstrAlloc's instrument_options bitmask */
typedef enum InstrumentOption
{
	INSTRUMENT_TIMER = 1 << 0,	/* needs timer (and row counts) */
	INSTRUMENT_BUFFERS = 1 << 1,	/* needs buffer usage */
	INSTRUMENT_ROWS = 1 << 2,	/* needs row count */
	INSTRUMENT_ALL = PG_INT32_MAX
} InstrumentOption;

typedef struct Instrumentation
{
	/* Parameters set at node creation: */
	bool		need_timer;		/* TRUE if we need timer data */
	bool		need_bufusage;	/* TRUE if we need buffer usage data */
	/* Info about current plan cycle: */
	bool		running;		/* TRUE if we've completed first tuple */
	instr_time	starttime;		/* Start time of current iteration of node */
	instr_time	counter;		/* Accumulated runtime for this node */
	double		firsttuple;		/* Time for first tuple of this cycle */
	double		tuplecount;		/* Tuples emitted so far this cycle */
	BufferUsage bufusage_start; /* Buffer usage at start */
	/* Accumulated statistics across all completed cycles: */
	double		startup;		/* Total startup time (in seconds) */
	double		total;			/* Total total time (in seconds) */
	double		ntuples;		/* Total tuples produced */
	double		nloops;			/* # of run cycles for this node */
	double		nfiltered1;		/* # tuples removed by scanqual or joinqual */
	double		nfiltered2;		/* # tuples removed by "other" quals */
	BufferUsage bufusage;		/* Total buffer usage */
} Instrumentation;

extern PGDLLIMPORT BufferUsage pgBufferUsage;

extern Instrumentation *InstrAlloc(int n, int instrument_options);
extern void InstrStartNode(Instrumentation *instr);
extern void InstrStopNode(Instrumentation *instr, double nTuples);
extern void InstrEndLoop(Instrumentation *instr);

#endif   /* INSTRUMENT_H */
