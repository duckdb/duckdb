/*-------------------------------------------------------------------------
 *
 * trigger.h
 *	  Declarations for trigger handling.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/trigger.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TRIGGER_H
#define TRIGGER_H

#include "catalog/objectaddress.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"

/*
 * TriggerData is the node type that is passed as fmgr "context" info
 * when a function is called by the trigger manager.
 */

#define CALLED_AS_TRIGGER(fcinfo) \
	((fcinfo)->context != NULL && IsA((fcinfo)->context, TriggerData))

typedef uint32 TriggerEvent;

typedef struct TriggerData
{
	NodeTag		type;
	TriggerEvent tg_event;
	Relation	tg_relation;
	HeapTuple	tg_trigtuple;
	HeapTuple	tg_newtuple;
	Trigger    *tg_trigger;
	Buffer		tg_trigtuplebuf;
	Buffer		tg_newtuplebuf;
} TriggerData;

/*
 * TriggerEvent bit flags
 *
 * Note that we assume different event types (INSERT/DELETE/UPDATE/TRUNCATE)
 * can't be OR'd together in a single TriggerEvent.  This is unlike the
 * situation for pg_trigger rows, so pg_trigger.tgtype uses a different
 * representation!
 */
#define TRIGGER_EVENT_INSERT			0x00000000
#define TRIGGER_EVENT_DELETE			0x00000001
#define TRIGGER_EVENT_UPDATE			0x00000002
#define TRIGGER_EVENT_TRUNCATE			0x00000003
#define TRIGGER_EVENT_OPMASK			0x00000003

#define TRIGGER_EVENT_ROW				0x00000004

#define TRIGGER_EVENT_BEFORE			0x00000008
#define TRIGGER_EVENT_AFTER				0x00000000
#define TRIGGER_EVENT_INSTEAD			0x00000010
#define TRIGGER_EVENT_TIMINGMASK		0x00000018

/* More TriggerEvent flags, used only within trigger.c */

#define AFTER_TRIGGER_DEFERRABLE		0x00000020
#define AFTER_TRIGGER_INITDEFERRED		0x00000040

#define TRIGGER_FIRED_BY_INSERT(event) \
	(((event) & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_INSERT)

#define TRIGGER_FIRED_BY_DELETE(event) \
	(((event) & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_DELETE)

#define TRIGGER_FIRED_BY_UPDATE(event) \
	(((event) & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_UPDATE)

#define TRIGGER_FIRED_BY_TRUNCATE(event) \
	(((event) & TRIGGER_EVENT_OPMASK) == TRIGGER_EVENT_TRUNCATE)

#define TRIGGER_FIRED_FOR_ROW(event) \
	((event) & TRIGGER_EVENT_ROW)

#define TRIGGER_FIRED_FOR_STATEMENT(event) \
	(!TRIGGER_FIRED_FOR_ROW(event))

#define TRIGGER_FIRED_BEFORE(event) \
	(((event) & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_BEFORE)

#define TRIGGER_FIRED_AFTER(event) \
	(((event) & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_AFTER)

#define TRIGGER_FIRED_INSTEAD(event) \
	(((event) & TRIGGER_EVENT_TIMINGMASK) == TRIGGER_EVENT_INSTEAD)

/*
 * Definitions for replication role based firing.
 */
#define SESSION_REPLICATION_ROLE_ORIGIN		0
#define SESSION_REPLICATION_ROLE_REPLICA	1
#define SESSION_REPLICATION_ROLE_LOCAL		2
extern PGDLLIMPORT int SessionReplicationRole;

/*
 * States at which a trigger can be fired. These are the
 * possible values for pg_trigger.tgenabled.
 */
#define TRIGGER_FIRES_ON_ORIGIN				'O'
#define TRIGGER_FIRES_ALWAYS				'A'
#define TRIGGER_FIRES_ON_REPLICA			'R'
#define TRIGGER_DISABLED					'D'

extern ObjectAddress CreateTrigger(CreateTrigStmt *stmt, const char *queryString,
			  Oid relOid, Oid refRelOid, Oid constraintOid, Oid indexOid,
			  bool isInternal);

extern void RemoveTriggerById(Oid trigOid);
extern Oid	get_trigger_oid(Oid relid, const char *name, bool missing_ok);

extern ObjectAddress renametrig(RenameStmt *stmt);

extern void EnableDisableTrigger(Relation rel, const char *tgname,
					 char fires_when, bool skip_system);

extern void RelationBuildTriggers(Relation relation);

extern TriggerDesc *CopyTriggerDesc(TriggerDesc *trigdesc);

extern void FreeTriggerDesc(TriggerDesc *trigdesc);

extern void ExecBSInsertTriggers(EState *estate,
					 ResultRelInfo *relinfo);
extern void ExecASInsertTriggers(EState *estate,
					 ResultRelInfo *relinfo);
extern TupleTableSlot *ExecBRInsertTriggers(EState *estate,
					 ResultRelInfo *relinfo,
					 TupleTableSlot *slot);
extern void ExecARInsertTriggers(EState *estate,
					 ResultRelInfo *relinfo,
					 HeapTuple trigtuple,
					 List *recheckIndexes);
extern TupleTableSlot *ExecIRInsertTriggers(EState *estate,
					 ResultRelInfo *relinfo,
					 TupleTableSlot *slot);
extern void ExecBSDeleteTriggers(EState *estate,
					 ResultRelInfo *relinfo);
extern void ExecASDeleteTriggers(EState *estate,
					 ResultRelInfo *relinfo);
extern bool ExecBRDeleteTriggers(EState *estate,
					 EPQState *epqstate,
					 ResultRelInfo *relinfo,
					 ItemPointer tupleid,
					 HeapTuple fdw_trigtuple);
extern void ExecARDeleteTriggers(EState *estate,
					 ResultRelInfo *relinfo,
					 ItemPointer tupleid,
					 HeapTuple fdw_trigtuple);
extern bool ExecIRDeleteTriggers(EState *estate,
					 ResultRelInfo *relinfo,
					 HeapTuple trigtuple);
extern void ExecBSUpdateTriggers(EState *estate,
					 ResultRelInfo *relinfo);
extern void ExecASUpdateTriggers(EState *estate,
					 ResultRelInfo *relinfo);
extern TupleTableSlot *ExecBRUpdateTriggers(EState *estate,
					 EPQState *epqstate,
					 ResultRelInfo *relinfo,
					 ItemPointer tupleid,
					 HeapTuple fdw_trigtuple,
					 TupleTableSlot *slot);
extern void ExecARUpdateTriggers(EState *estate,
					 ResultRelInfo *relinfo,
					 ItemPointer tupleid,
					 HeapTuple fdw_trigtuple,
					 HeapTuple newtuple,
					 List *recheckIndexes);
extern TupleTableSlot *ExecIRUpdateTriggers(EState *estate,
					 ResultRelInfo *relinfo,
					 HeapTuple trigtuple,
					 TupleTableSlot *slot);
extern void ExecBSTruncateTriggers(EState *estate,
					   ResultRelInfo *relinfo);
extern void ExecASTruncateTriggers(EState *estate,
					   ResultRelInfo *relinfo);

extern void AfterTriggerBeginXact(void);
extern void AfterTriggerBeginQuery(void);
extern void AfterTriggerEndQuery(EState *estate);
extern void AfterTriggerFireDeferred(void);
extern void AfterTriggerEndXact(bool isCommit);
extern void AfterTriggerBeginSubXact(void);
extern void AfterTriggerEndSubXact(bool isCommit);
extern void AfterTriggerSetState(ConstraintsSetStmt *stmt);
extern bool AfterTriggerPendingOnRel(Oid relid);


/*
 * in utils/adt/ri_triggers.c
 */
extern bool RI_FKey_pk_upd_check_required(Trigger *trigger, Relation pk_rel,
							  HeapTuple old_row, HeapTuple new_row);
extern bool RI_FKey_fk_upd_check_required(Trigger *trigger, Relation fk_rel,
							  HeapTuple old_row, HeapTuple new_row);
extern bool RI_Initial_Check(Trigger *trigger,
				 Relation fk_rel, Relation pk_rel);

/* result values for RI_FKey_trigger_type: */
#define RI_TRIGGER_PK	1		/* is a trigger on the PK relation */
#define RI_TRIGGER_FK	2		/* is a trigger on the FK relation */
#define RI_TRIGGER_NONE 0		/* is not an RI trigger function */

extern int	RI_FKey_trigger_type(Oid tgfoid);

extern Datum pg_trigger_depth(PG_FUNCTION_ARGS);

#endif   /* TRIGGER_H */
