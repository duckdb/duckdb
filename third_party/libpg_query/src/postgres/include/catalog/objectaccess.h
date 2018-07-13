/*
 * objectaccess.h
 *
 *		Object access hooks.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */

#ifndef OBJECTACCESS_H
#define OBJECTACCESS_H

/*
 * Object access hooks are intended to be called just before or just after
 * performing certain actions on a SQL object.  This is intended as
 * infrastructure for security or logging plugins.
 *
 * OAT_POST_CREATE should be invoked just after the object is created.
 * Typically, this is done after inserting the primary catalog records and
 * associated dependencies.
 *
 * OAT_DROP should be invoked just before deletion of objects; typically
 * deleteOneObject(). Its arguments are packed within ObjectAccessDrop.
 *
 * OAT_POST_ALTER should be invoked just after the object is altered,
 * but before the command counter is incremented.  An extension using the
 * hook can use a current MVCC snapshot to get the old version of the tuple,
 * and can use SnapshotSelf to get the new version of the tuple.
 *
 * OAT_NAMESPACE_SEARCH should be invoked prior to object name lookup under
 * a particular namespace. This event is equivalent to usage permission
 * on a schema under the default access control mechanism.
 *
 * OAT_FUNCTION_EXECUTE should be invoked prior to function execution.
 * This event is almost equivalent to execute permission on functions,
 * except for the case when execute permission is checked during object
 * creation or altering, because OAT_POST_CREATE or OAT_POST_ALTER are
 * sufficient for extensions to track these kind of checks.
 *
 * Other types may be added in the future.
 */
typedef enum ObjectAccessType
{
	OAT_POST_CREATE,
	OAT_DROP,
	OAT_POST_ALTER,
	OAT_NAMESPACE_SEARCH,
	OAT_FUNCTION_EXECUTE
} ObjectAccessType;

/*
 * Arguments of OAT_POST_CREATE event
 */
typedef struct
{
	/*
	 * This flag informs extensions whether the context of this creation is
	 * invoked by user's operations, or not. E.g, it shall be dealt as
	 * internal stuff on toast tables or indexes due to type changes.
	 */
	bool		is_internal;
} ObjectAccessPostCreate;

/*
 * Arguments of OAT_DROP event
 */
typedef struct
{
	/*
	 * Flags to inform extensions the context of this deletion. Also see
	 * PERFORM_DELETION_* in dependency.h
	 */
	int			dropflags;
} ObjectAccessDrop;

/*
 * Arguments of OAT_POST_ALTER event
 */
typedef struct
{
	/*
	 * This identifier is used when system catalog takes two IDs to identify a
	 * particular tuple of the catalog. It is only used when the caller want
	 * to identify an entry of pg_inherits, pg_db_role_setting or
	 * pg_user_mapping. Elsewhere, InvalidOid should be set.
	 */
	Oid			auxiliary_id;

	/*
	 * If this flag is set, the user hasn't requested that the object be
	 * altered, but we're doing it anyway for some internal reason.
	 * Permissions-checking hooks may want to skip checks if, say, we're alter
	 * the constraints of a temporary heap during CLUSTER.
	 */
	bool		is_internal;
} ObjectAccessPostAlter;

/*
 * Arguments of OAT_NAMESPACE_SEARCH
 */
typedef struct
{
	/*
	 * If true, hook should report an error when permission to search this
	 * schema is denied.
	 */
	bool		ereport_on_violation;

	/*
	 * This is, in essence, an out parameter.  Core code should initialize
	 * this to true, and any extension that wants to deny access should reset
	 * it to false.  But an extension should be careful never to store a true
	 * value here, so that in case there are multiple extensions access is
	 * only allowed if all extensions agree.
	 */
	bool		result;
} ObjectAccessNamespaceSearch;

/* Plugin provides a hook function matching this signature. */
typedef void (*object_access_hook_type) (ObjectAccessType access,
													 Oid classId,
													 Oid objectId,
													 int subId,
													 void *arg);

/* Plugin sets this variable to a suitable hook function. */
extern PGDLLIMPORT object_access_hook_type object_access_hook;

/* Core code uses these functions to call the hook (see macros below). */
extern void RunObjectPostCreateHook(Oid classId, Oid objectId, int subId,
						bool is_internal);
extern void RunObjectDropHook(Oid classId, Oid objectId, int subId,
				  int dropflags);
extern void RunObjectPostAlterHook(Oid classId, Oid objectId, int subId,
					   Oid auxiliaryId, bool is_internal);
extern bool RunNamespaceSearchHook(Oid objectId, bool ereport_on_volation);
extern void RunFunctionExecuteHook(Oid objectId);

/*
 * The following macros are wrappers around the functions above; these should
 * normally be used to invoke the hook in lieu of calling the above functions
 * directly.
 */

#define InvokeObjectPostCreateHook(classId,objectId,subId)			\
	InvokeObjectPostCreateHookArg((classId),(objectId),(subId),false)
#define InvokeObjectPostCreateHookArg(classId,objectId,subId,is_internal) \
	do {															\
		if (object_access_hook)										\
			RunObjectPostCreateHook((classId),(objectId),(subId),	\
									(is_internal));					\
	} while(0)

#define InvokeObjectDropHook(classId,objectId,subId)				\
	InvokeObjectDropHookArg((classId),(objectId),(subId),0)
#define InvokeObjectDropHookArg(classId,objectId,subId,dropflags)	\
	do {															\
		if (object_access_hook)										\
			RunObjectDropHook((classId),(objectId),(subId),			\
							  (dropflags));							\
	} while(0)

#define InvokeObjectPostAlterHook(classId,objectId,subId)			\
	InvokeObjectPostAlterHookArg((classId),(objectId),(subId),		\
								 InvalidOid,false)
#define InvokeObjectPostAlterHookArg(classId,objectId,subId,		\
									 auxiliaryId,is_internal)		\
	do {															\
		if (object_access_hook)										\
			RunObjectPostAlterHook((classId),(objectId),(subId),	\
								   (auxiliaryId),(is_internal));	\
	} while(0)

#define InvokeNamespaceSearchHook(objectId, ereport_on_violation)	\
	(!object_access_hook											\
	 ? true															\
	 : RunNamespaceSearchHook((objectId), (ereport_on_violation)))

#define InvokeFunctionExecuteHook(objectId)		\
	do {										\
		if (object_access_hook)					\
			RunFunctionExecuteHook(objectId);	\
	} while(0)

#endif   /* OBJECTACCESS_H */
