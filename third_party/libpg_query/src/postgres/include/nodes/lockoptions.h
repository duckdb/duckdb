/*-------------------------------------------------------------------------
 *
 * lockoptions.h
 *	  Common header for some locking-related declarations.
 *
 *
 * Copyright (c) 2014-2015, PostgreSQL Global Development Group
 *
 * src/include/nodes/lockoptions.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOCKOPTIONS_H
#define LOCKOPTIONS_H

/*
 * This enum represents the different strengths of FOR UPDATE/SHARE clauses.
 * The ordering here is important, because the highest numerical value takes
 * precedence when a RTE is specified multiple ways.  See applyLockingClause.
 */
typedef enum LockClauseStrength
{
	LCS_NONE,					/* no such clause - only used in PlanRowMark */
	LCS_FORKEYSHARE,			/* FOR KEY SHARE */
	LCS_FORSHARE,				/* FOR SHARE */
	LCS_FORNOKEYUPDATE,			/* FOR NO KEY UPDATE */
	LCS_FORUPDATE				/* FOR UPDATE */
} LockClauseStrength;

/*
 * This enum controls how to deal with rows being locked by FOR UPDATE/SHARE
 * clauses (i.e., it represents the NOWAIT and SKIP LOCKED options).
 * The ordering here is important, because the highest numerical value takes
 * precedence when a RTE is specified multiple ways.  See applyLockingClause.
 */
typedef enum LockWaitPolicy
{
	/* Wait for the lock to become available (default behavior) */
	LockWaitBlock,
	/* Skip rows that can't be locked (SKIP LOCKED) */
	LockWaitSkip,
	/* Raise an error if a row cannot be locked (NOWAIT) */
	LockWaitError
} LockWaitPolicy;

#endif   /* LOCKOPTIONS_H */
