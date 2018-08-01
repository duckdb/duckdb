#ifndef PG_TRIGGER_H
#define PG_TRIGGER_H

/* ----------------
 *		compiler constants for pg_trigger
 * ----------------
 */
#define Natts_pg_trigger 15
#define Anum_pg_trigger_tgrelid 1
#define Anum_pg_trigger_tgname 2
#define Anum_pg_trigger_tgfoid 3
#define Anum_pg_trigger_tgtype 4
#define Anum_pg_trigger_tgenabled 5
#define Anum_pg_trigger_tgisinternal 6
#define Anum_pg_trigger_tgconstrrelid 7
#define Anum_pg_trigger_tgconstrindid 8
#define Anum_pg_trigger_tgconstraint 9
#define Anum_pg_trigger_tgdeferrable 10
#define Anum_pg_trigger_tginitdeferred 11
#define Anum_pg_trigger_tgnargs 12
#define Anum_pg_trigger_tgattr 13
#define Anum_pg_trigger_tgargs 14
#define Anum_pg_trigger_tgqual 15

/* Bits within tgtype */
#define TRIGGER_TYPE_ROW (1 << 0)
#define TRIGGER_TYPE_BEFORE (1 << 1)
#define TRIGGER_TYPE_INSERT (1 << 2)
#define TRIGGER_TYPE_DELETE (1 << 3)
#define TRIGGER_TYPE_UPDATE (1 << 4)
#define TRIGGER_TYPE_TRUNCATE (1 << 5)
#define TRIGGER_TYPE_INSTEAD (1 << 6)
#define TRIGGER_TYPE_COMMIT (1 << 7)

#define TRIGGER_TYPE_LEVEL_MASK (TRIGGER_TYPE_ROW)
#define TRIGGER_TYPE_STATEMENT 0

/* Note bits within TRIGGER_TYPE_TIMING_MASK aren't adjacent */
#define TRIGGER_TYPE_TIMING_MASK                                               \
	(TRIGGER_TYPE_BEFORE | TRIGGER_TYPE_INSTEAD | TRIGGER_TYPE_COMMIT)
#define TRIGGER_TYPE_AFTER 0

#define TRIGGER_TYPE_EVENT_MASK                                                \
	(TRIGGER_TYPE_INSERT | TRIGGER_TYPE_DELETE | TRIGGER_TYPE_UPDATE |         \
	 TRIGGER_TYPE_TRUNCATE)

/* Macros for manipulating tgtype */
#define TRIGGER_CLEAR_TYPE(type) ((type) = 0)

#define TRIGGER_SETT_ROW(type) ((type) |= TRIGGER_TYPE_ROW)
#define TRIGGER_SETT_STATEMENT(type) ((type) |= TRIGGER_TYPE_STATEMENT)
#define TRIGGER_SETT_BEFORE(type) ((type) |= TRIGGER_TYPE_BEFORE)
#define TRIGGER_SETT_AFTER(type) ((type) |= TRIGGER_TYPE_AFTER)
#define TRIGGER_SETT_INSTEAD(type) ((type) |= TRIGGER_TYPE_INSTEAD)
#define TRIGGER_SETT_INSERT(type) ((type) |= TRIGGER_TYPE_INSERT)
#define TRIGGER_SETT_DELETE(type) ((type) |= TRIGGER_TYPE_DELETE)
#define TRIGGER_SETT_UPDATE(type) ((type) |= TRIGGER_TYPE_UPDATE)
#define TRIGGER_SETT_TRUNCATE(type) ((type) |= TRIGGER_TYPE_TRUNCATE)

#define TRIGGER_FOR_ROW(type) ((type)&TRIGGER_TYPE_ROW)
#define TRIGGER_FOR_BEFORE(type)                                               \
	(((type)&TRIGGER_TYPE_TIMING_MASK) == TRIGGER_TYPE_BEFORE)
#define TRIGGER_FOR_AFTER(type)                                                \
	(((type)&TRIGGER_TYPE_TIMING_MASK) == TRIGGER_TYPE_AFTER)
#define TRIGGER_FOR_INSTEAD(type)                                              \
	(((type)&TRIGGER_TYPE_TIMING_MASK) == TRIGGER_TYPE_INSTEAD)
#define TRIGGER_FOR_INSERT(type) ((type)&TRIGGER_TYPE_INSERT)
#define TRIGGER_FOR_DELETE(type) ((type)&TRIGGER_TYPE_DELETE)
#define TRIGGER_FOR_UPDATE(type) ((type)&TRIGGER_TYPE_UPDATE)
#define TRIGGER_FOR_TRUNCATE(type) ((type)&TRIGGER_TYPE_TRUNCATE)

/*
 * Efficient macro for checking if tgtype matches a particular level
 * (TRIGGER_TYPE_ROW or TRIGGER_TYPE_STATEMENT), timing (TRIGGER_TYPE_BEFORE,
 * TRIGGER_TYPE_AFTER or TRIGGER_TYPE_INSTEAD), and event (TRIGGER_TYPE_INSERT,
 * TRIGGER_TYPE_DELETE, TRIGGER_TYPE_UPDATE, or TRIGGER_TYPE_TRUNCATE).  Note
 * that a tgtype can match more than one event, but only one level or timing.
 */
#define TRIGGER_TYPE_MATCHES(type, level, timing, event)                       \
	(((type) & (TRIGGER_TYPE_LEVEL_MASK | TRIGGER_TYPE_TIMING_MASK |           \
	            (event))) == ((level) | (timing) | (event)))

#endif /* PG_TRIGGER_H */
