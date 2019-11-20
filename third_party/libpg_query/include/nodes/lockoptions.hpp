/*-------------------------------------------------------------------------
 *
 * lockoptions.h
 *	  Common header for some locking-related declarations.
 *
 *
 * Copyright (c) 2014-2017, PostgreSQL Global Development PGGroup
 *
 * src/include/nodes/lockoptions.h
 *
 *-------------------------------------------------------------------------
 */
#pragma once

/*
 * This enum represents the different strengths of FOR UPDATE/SHARE clauses.
 * The ordering here is important, because the highest numerical value takes
 * precedence when a RTE is specified multiple ways.  See applyLockingClause.
 */
typedef enum PGLockClauseStrength
{
	PG_LCS_NONE,					/* no such clause - only used in PGPlanRowMark */
	PG_LCS_FORKEYSHARE,			/* FOR KEY SHARE */
	PG_LCS_FORSHARE,				/* FOR SHARE */
	PG_LCS_FORNOKEYUPDATE,			/* FOR NO KEY UPDATE */
	LCS_FORUPDATE				/* FOR UPDATE */
} PGLockClauseStrength;

/*
 * This enum controls how to deal with rows being locked by FOR UPDATE/SHARE
 * clauses (i.e., it represents the NOWAIT and SKIP LOCKED options).
 * The ordering here is important, because the highest numerical value takes
 * precedence when a RTE is specified multiple ways.  See applyLockingClause.
 */
typedef enum PGLockWaitPolicy
{
	/* Wait for the lock to become available (default behavior) */
	PGLockWaitBlock,
	/* Skip rows that can't be locked (SKIP LOCKED) */
	PGLockWaitSkip,
	/* Raise an error if a row cannot be locked (NOWAIT) */
	LockWaitError
} PGLockWaitPolicy;

