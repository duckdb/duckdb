/*-------------------------------------------------------------------------
 *
 * prs2lock.h
 *	  data structures for POSTGRES Rule System II (rewrite rules only)
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/rewrite/prs2lock.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRS2LOCK_H
#define PRS2LOCK_H

#include "access/attnum.h"
#include "nodes/pg_list.h"

/*
 * RewriteRule -
 *	  holds an info for a rewrite rule
 *
 */
typedef struct RewriteRule
{
	Oid			ruleId;
	CmdType		event;
	Node	   *qual;
	List	   *actions;
	char		enabled;
	bool		isInstead;
} RewriteRule;

/*
 * RuleLock -
 *	  all rules that apply to a particular relation. Even though we only
 *	  have the rewrite rule system left and these are not really "locks",
 *	  the name is kept for historical reasons.
 */
typedef struct RuleLock
{
	int			numLocks;
	RewriteRule **rules;
} RuleLock;

#endif   /* REWRITE_H */
