/*-------------------------------------------------------------------------
 *
 * spi.h
 *				Server Programming Interface public declarations
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/spi.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SPI_H
#define SPI_H

#include "lib/ilist.h"
#include "nodes/parsenodes.h"
#include "utils/portal.h"


typedef struct SPITupleTable
{
	MemoryContext tuptabcxt;	/* memory context of result table */
	uint32		alloced;		/* # of alloced vals */
	uint32		free;			/* # of free vals */
	TupleDesc	tupdesc;		/* tuple descriptor */
	HeapTuple  *vals;			/* tuples */
	slist_node	next;			/* link for internal bookkeeping */
	SubTransactionId subid;		/* subxact in which tuptable was created */
} SPITupleTable;

/* Plans are opaque structs for standard users of SPI */
typedef struct _SPI_plan *SPIPlanPtr;

#define SPI_ERROR_CONNECT		(-1)
#define SPI_ERROR_COPY			(-2)
#define SPI_ERROR_OPUNKNOWN		(-3)
#define SPI_ERROR_UNCONNECTED	(-4)
#define SPI_ERROR_CURSOR		(-5)	/* not used anymore */
#define SPI_ERROR_ARGUMENT		(-6)
#define SPI_ERROR_PARAM			(-7)
#define SPI_ERROR_TRANSACTION	(-8)
#define SPI_ERROR_NOATTRIBUTE	(-9)
#define SPI_ERROR_NOOUTFUNC		(-10)
#define SPI_ERROR_TYPUNKNOWN	(-11)

#define SPI_OK_CONNECT			1
#define SPI_OK_FINISH			2
#define SPI_OK_FETCH			3
#define SPI_OK_UTILITY			4
#define SPI_OK_SELECT			5
#define SPI_OK_SELINTO			6
#define SPI_OK_INSERT			7
#define SPI_OK_DELETE			8
#define SPI_OK_UPDATE			9
#define SPI_OK_CURSOR			10
#define SPI_OK_INSERT_RETURNING 11
#define SPI_OK_DELETE_RETURNING 12
#define SPI_OK_UPDATE_RETURNING 13
#define SPI_OK_REWRITTEN		14

extern PGDLLIMPORT uint32 SPI_processed;
extern PGDLLIMPORT Oid SPI_lastoid;
extern PGDLLIMPORT SPITupleTable *SPI_tuptable;
extern PGDLLIMPORT int SPI_result;

extern int	SPI_connect(void);
extern int	SPI_finish(void);
extern void SPI_push(void);
extern void SPI_pop(void);
extern bool SPI_push_conditional(void);
extern void SPI_pop_conditional(bool pushed);
extern void SPI_restore_connection(void);
extern int	SPI_execute(const char *src, bool read_only, long tcount);
extern int SPI_execute_plan(SPIPlanPtr plan, Datum *Values, const char *Nulls,
				 bool read_only, long tcount);
extern int SPI_execute_plan_with_paramlist(SPIPlanPtr plan,
								ParamListInfo params,
								bool read_only, long tcount);
extern int	SPI_exec(const char *src, long tcount);
extern int SPI_execp(SPIPlanPtr plan, Datum *Values, const char *Nulls,
		  long tcount);
extern int SPI_execute_snapshot(SPIPlanPtr plan,
					 Datum *Values, const char *Nulls,
					 Snapshot snapshot,
					 Snapshot crosscheck_snapshot,
					 bool read_only, bool fire_triggers, long tcount);
extern int SPI_execute_with_args(const char *src,
					  int nargs, Oid *argtypes,
					  Datum *Values, const char *Nulls,
					  bool read_only, long tcount);
extern SPIPlanPtr SPI_prepare(const char *src, int nargs, Oid *argtypes);
extern SPIPlanPtr SPI_prepare_cursor(const char *src, int nargs, Oid *argtypes,
				   int cursorOptions);
extern SPIPlanPtr SPI_prepare_params(const char *src,
				   ParserSetupHook parserSetup,
				   void *parserSetupArg,
				   int cursorOptions);
extern int	SPI_keepplan(SPIPlanPtr plan);
extern SPIPlanPtr SPI_saveplan(SPIPlanPtr plan);
extern int	SPI_freeplan(SPIPlanPtr plan);

extern Oid	SPI_getargtypeid(SPIPlanPtr plan, int argIndex);
extern int	SPI_getargcount(SPIPlanPtr plan);
extern bool SPI_is_cursor_plan(SPIPlanPtr plan);
extern bool SPI_plan_is_valid(SPIPlanPtr plan);
extern const char *SPI_result_code_string(int code);

extern List *SPI_plan_get_plan_sources(SPIPlanPtr plan);
extern CachedPlan *SPI_plan_get_cached_plan(SPIPlanPtr plan);

extern HeapTuple SPI_copytuple(HeapTuple tuple);
extern HeapTupleHeader SPI_returntuple(HeapTuple tuple, TupleDesc tupdesc);
extern HeapTuple SPI_modifytuple(Relation rel, HeapTuple tuple, int natts,
				int *attnum, Datum *Values, const char *Nulls);
extern int	SPI_fnumber(TupleDesc tupdesc, const char *fname);
extern char *SPI_fname(TupleDesc tupdesc, int fnumber);
extern char *SPI_getvalue(HeapTuple tuple, TupleDesc tupdesc, int fnumber);
extern Datum SPI_getbinval(HeapTuple tuple, TupleDesc tupdesc, int fnumber, bool *isnull);
extern char *SPI_gettype(TupleDesc tupdesc, int fnumber);
extern Oid	SPI_gettypeid(TupleDesc tupdesc, int fnumber);
extern char *SPI_getrelname(Relation rel);
extern char *SPI_getnspname(Relation rel);
extern void *SPI_palloc(Size size);
extern void *SPI_repalloc(void *pointer, Size size);
extern void SPI_pfree(void *pointer);
extern Datum SPI_datumTransfer(Datum value, bool typByVal, int typLen);
extern void SPI_freetuple(HeapTuple pointer);
extern void SPI_freetuptable(SPITupleTable *tuptable);

extern Portal SPI_cursor_open(const char *name, SPIPlanPtr plan,
				Datum *Values, const char *Nulls, bool read_only);
extern Portal SPI_cursor_open_with_args(const char *name,
						  const char *src,
						  int nargs, Oid *argtypes,
						  Datum *Values, const char *Nulls,
						  bool read_only, int cursorOptions);
extern Portal SPI_cursor_open_with_paramlist(const char *name, SPIPlanPtr plan,
							   ParamListInfo params, bool read_only);
extern Portal SPI_cursor_find(const char *name);
extern void SPI_cursor_fetch(Portal portal, bool forward, long count);
extern void SPI_cursor_move(Portal portal, bool forward, long count);
extern void SPI_scroll_cursor_fetch(Portal, FetchDirection direction, long count);
extern void SPI_scroll_cursor_move(Portal, FetchDirection direction, long count);
extern void SPI_cursor_close(Portal portal);

extern void AtEOXact_SPI(bool isCommit);
extern void AtEOSubXact_SPI(bool isCommit, SubTransactionId mySubid);

#endif   /* SPI_H */
