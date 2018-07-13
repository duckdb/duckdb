/*-------------------------------------------------------------------------
 *
 * timeout.h
 *	  Routines to multiplex SIGALRM interrupts for multiple timeout reasons.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/timeout.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TIMEOUT_H
#define TIMEOUT_H

#include "datatype/timestamp.h"

/*
 * Identifiers for timeout reasons.  Note that in case multiple timeouts
 * trigger at the same time, they are serviced in the order of this enum.
 */
typedef enum TimeoutId
{
	/* Predefined timeout reasons */
	STARTUP_PACKET_TIMEOUT,
	DEADLOCK_TIMEOUT,
	LOCK_TIMEOUT,
	STATEMENT_TIMEOUT,
	STANDBY_DEADLOCK_TIMEOUT,
	STANDBY_TIMEOUT,
	/* First user-definable timeout reason */
	USER_TIMEOUT,
	/* Maximum number of timeout reasons */
	MAX_TIMEOUTS = 16
} TimeoutId;

/* callback function signature */
typedef void (*timeout_handler_proc) (void);

/*
 * Parameter structure for setting multiple timeouts at once
 */
typedef enum TimeoutType
{
	TMPARAM_AFTER,
	TMPARAM_AT
} TimeoutType;

typedef struct
{
	TimeoutId	id;				/* timeout to set */
	TimeoutType type;			/* TMPARAM_AFTER or TMPARAM_AT */
	int			delay_ms;		/* only used for TMPARAM_AFTER */
	TimestampTz fin_time;		/* only used for TMPARAM_AT */
} EnableTimeoutParams;

/*
 * Parameter structure for clearing multiple timeouts at once
 */
typedef struct
{
	TimeoutId	id;				/* timeout to clear */
	bool		keep_indicator; /* keep the indicator flag? */
} DisableTimeoutParams;

/* timeout setup */
extern void InitializeTimeouts(void);
extern TimeoutId RegisterTimeout(TimeoutId id, timeout_handler_proc handler);
extern void reschedule_timeouts(void);

/* timeout operation */
extern void enable_timeout_after(TimeoutId id, int delay_ms);
extern void enable_timeout_at(TimeoutId id, TimestampTz fin_time);
extern void enable_timeouts(const EnableTimeoutParams *timeouts, int count);
extern void disable_timeout(TimeoutId id, bool keep_indicator);
extern void disable_timeouts(const DisableTimeoutParams *timeouts, int count);
extern void disable_all_timeouts(bool keep_indicators);

/* accessors */
extern bool get_timeout_indicator(TimeoutId id, bool reset_indicator);
extern TimestampTz get_timeout_start_time(TimeoutId id);

#endif   /* TIMEOUT_H */
