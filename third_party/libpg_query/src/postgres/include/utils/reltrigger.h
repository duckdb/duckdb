/*-------------------------------------------------------------------------
 *
 * reltrigger.h
 *	  POSTGRES relation trigger definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/reltrigger.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELTRIGGER_H
#define RELTRIGGER_H


/*
 * These struct really belongs to trigger.h, but we put it separately so that
 * it can be cleanly included in rel.h and other places.
 */

typedef struct Trigger
{
	Oid			tgoid;			/* OID of trigger (pg_trigger row) */
	/* Remaining fields are copied from pg_trigger, see pg_trigger.h */
	char	   *tgname;
	Oid			tgfoid;
	int16		tgtype;
	char		tgenabled;
	bool		tgisinternal;
	Oid			tgconstrrelid;
	Oid			tgconstrindid;
	Oid			tgconstraint;
	bool		tgdeferrable;
	bool		tginitdeferred;
	int16		tgnargs;
	int16		tgnattr;
	int16	   *tgattr;
	char	  **tgargs;
	char	   *tgqual;
} Trigger;

typedef struct TriggerDesc
{
	Trigger    *triggers;		/* array of Trigger structs */
	int			numtriggers;	/* number of array entries */

	/*
	 * These flags indicate whether the array contains at least one of each
	 * type of trigger.  We use these to skip searching the array if not.
	 */
	bool		trig_insert_before_row;
	bool		trig_insert_after_row;
	bool		trig_insert_instead_row;
	bool		trig_insert_before_statement;
	bool		trig_insert_after_statement;
	bool		trig_update_before_row;
	bool		trig_update_after_row;
	bool		trig_update_instead_row;
	bool		trig_update_before_statement;
	bool		trig_update_after_statement;
	bool		trig_delete_before_row;
	bool		trig_delete_after_row;
	bool		trig_delete_instead_row;
	bool		trig_delete_before_statement;
	bool		trig_delete_after_statement;
	/* there are no row-level truncate triggers */
	bool		trig_truncate_before_statement;
	bool		trig_truncate_after_statement;
} TriggerDesc;

#endif   /* RELTRIGGER_H */
