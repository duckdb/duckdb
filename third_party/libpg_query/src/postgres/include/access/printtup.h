/*-------------------------------------------------------------------------
 *
 * printtup.h
 *
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/printtup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRINTTUP_H
#define PRINTTUP_H

#include "utils/portal.h"

extern DestReceiver *printtup_create_DR(CommandDest dest);

extern void SetRemoteDestReceiverParams(DestReceiver *self, Portal portal);

extern void SendRowDescriptionMessage(TupleDesc typeinfo, List *targetlist,
						  int16 *formats);

extern void debugStartup(DestReceiver *self, int operation,
			 TupleDesc typeinfo);
extern void debugtup(TupleTableSlot *slot, DestReceiver *self);

/* XXX these are really in executor/spi.c */
extern void spi_dest_startup(DestReceiver *self, int operation,
				 TupleDesc typeinfo);
extern void spi_printtup(TupleTableSlot *slot, DestReceiver *self);

#endif   /* PRINTTUP_H */
