/*-------------------------------------------------------------------------
 *
 * rls.h
 *	  Header file for Row Level Security (RLS) utility commands to be used
 *	  with the rowsecurity feature.
 *
 * Copyright (c) 2007-2015, PostgreSQL Global Development Group
 *
 * src/include/utils/rls.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RLS_H
#define RLS_H

/* GUC variable */
extern bool row_security;

/*
 * Used by callers of check_enable_rls.
 *
 * RLS could be completely disabled on the tables involved in the query,
 * which is the simple case, or it may depend on the current environment
 * (the role which is running the query or the value of the row_security
 * GUC), or it might be simply enabled as usual.
 *
 * If RLS isn't on the table involved then RLS_NONE is returned to indicate
 * that we don't need to worry about invalidating the query plan for RLS
 * reasons.  If RLS is on the table, but we are bypassing it for now, then
 * we return RLS_NONE_ENV to indicate that, if the environment changes,
 * we need to invalidate and replan.  Finally, if RLS should be turned on
 * for the query, then we return RLS_ENABLED, which means we also need to
 * invalidate if the environment changes.
 *
 * Note that RLS_ENABLED will also be returned if noError is true
 * (indicating that the caller simply want to know if RLS should be applied
 * for this user but doesn't want an error thrown if it is; this is used
 * by other error cases where we're just trying to decide if data from the
 * table should be passed back to the user or not).
 */
enum CheckEnableRlsResult
{
	RLS_NONE,
	RLS_NONE_ENV,
	RLS_ENABLED
};

extern int	check_enable_rls(Oid relid, Oid checkAsUser, bool noError);

#endif   /* RLS_H */
