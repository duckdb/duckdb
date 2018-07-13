/*-------------------------------------------------------------------------
 *
 * rewriteSupport.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/rewrite/rewriteSupport.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REWRITESUPPORT_H
#define REWRITESUPPORT_H

/* The ON SELECT rule of a view is always named this: */
#define ViewSelectRuleName	"_RETURN"

extern bool IsDefinedRewriteRule(Oid owningRel, const char *ruleName);

extern void SetRelationRuleStatus(Oid relationId, bool relHasRules);

extern Oid	get_rewrite_oid(Oid relid, const char *rulename, bool missing_ok);
extern Oid get_rewrite_oid_without_relid(const char *rulename,
							  Oid *relid, bool missing_ok);

#endif   /* REWRITESUPPORT_H */
