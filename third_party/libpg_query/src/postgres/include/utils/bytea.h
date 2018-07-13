/*-------------------------------------------------------------------------
 *
 * bytea.h
 *	  Declarations for BYTEA data type support.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/bytea.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BYTEA_H
#define BYTEA_H

#include "fmgr.h"


typedef enum
{
	BYTEA_OUTPUT_ESCAPE,
	BYTEA_OUTPUT_HEX
}	ByteaOutputType;

extern int	bytea_output;		/* ByteaOutputType, but int for GUC enum */

/* functions are in utils/adt/varlena.c */
extern Datum byteain(PG_FUNCTION_ARGS);
extern Datum byteaout(PG_FUNCTION_ARGS);
extern Datum bytearecv(PG_FUNCTION_ARGS);
extern Datum byteasend(PG_FUNCTION_ARGS);
extern Datum byteaoctetlen(PG_FUNCTION_ARGS);
extern Datum byteaGetByte(PG_FUNCTION_ARGS);
extern Datum byteaGetBit(PG_FUNCTION_ARGS);
extern Datum byteaSetByte(PG_FUNCTION_ARGS);
extern Datum byteaSetBit(PG_FUNCTION_ARGS);
extern Datum byteaeq(PG_FUNCTION_ARGS);
extern Datum byteane(PG_FUNCTION_ARGS);
extern Datum bytealt(PG_FUNCTION_ARGS);
extern Datum byteale(PG_FUNCTION_ARGS);
extern Datum byteagt(PG_FUNCTION_ARGS);
extern Datum byteage(PG_FUNCTION_ARGS);
extern Datum byteacmp(PG_FUNCTION_ARGS);
extern Datum byteacat(PG_FUNCTION_ARGS);
extern Datum byteapos(PG_FUNCTION_ARGS);
extern Datum bytea_substr(PG_FUNCTION_ARGS);
extern Datum bytea_substr_no_len(PG_FUNCTION_ARGS);
extern Datum byteaoverlay(PG_FUNCTION_ARGS);
extern Datum byteaoverlay_no_len(PG_FUNCTION_ARGS);

#endif   /* BYTEA_H */
