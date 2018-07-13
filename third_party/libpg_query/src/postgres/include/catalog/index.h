/*-------------------------------------------------------------------------
 *
 * index.h
 *	  prototypes for catalog/index.c.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/index.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEX_H
#define INDEX_H

#include "catalog/objectaddress.h"
#include "nodes/execnodes.h"


#define DEFAULT_INDEX_TYPE	"btree"

/* Typedef for callback function for IndexBuildHeapScan */
typedef void (*IndexBuildCallback) (Relation index,
												HeapTuple htup,
												Datum *values,
												bool *isnull,
												bool tupleIsAlive,
												void *state);

/* Action code for index_set_state_flags */
typedef enum
{
	INDEX_CREATE_SET_READY,
	INDEX_CREATE_SET_VALID,
	INDEX_DROP_CLEAR_VALID,
	INDEX_DROP_SET_DEAD
} IndexStateFlagsAction;


extern void index_check_primary_key(Relation heapRel,
						IndexInfo *indexInfo,
						bool is_alter_table);

extern Oid index_create(Relation heapRelation,
			 const char *indexRelationName,
			 Oid indexRelationId,
			 Oid relFileNode,
			 IndexInfo *indexInfo,
			 List *indexColNames,
			 Oid accessMethodObjectId,
			 Oid tableSpaceId,
			 Oid *collationObjectId,
			 Oid *classObjectId,
			 int16 *coloptions,
			 Datum reloptions,
			 bool isprimary,
			 bool isconstraint,
			 bool deferrable,
			 bool initdeferred,
			 bool allow_system_table_mods,
			 bool skip_build,
			 bool concurrent,
			 bool is_internal,
			 bool if_not_exists);

extern ObjectAddress index_constraint_create(Relation heapRelation,
						Oid indexRelationId,
						IndexInfo *indexInfo,
						const char *constraintName,
						char constraintType,
						bool deferrable,
						bool initdeferred,
						bool mark_as_primary,
						bool update_pgindex,
						bool remove_old_dependencies,
						bool allow_system_table_mods,
						bool is_internal);

extern void index_drop(Oid indexId, bool concurrent);

extern IndexInfo *BuildIndexInfo(Relation index);

extern void BuildSpeculativeIndexInfo(Relation index, IndexInfo *ii);

extern void FormIndexDatum(IndexInfo *indexInfo,
			   TupleTableSlot *slot,
			   EState *estate,
			   Datum *values,
			   bool *isnull);

extern void index_build(Relation heapRelation,
			Relation indexRelation,
			IndexInfo *indexInfo,
			bool isprimary,
			bool isreindex);

extern double IndexBuildHeapScan(Relation heapRelation,
				   Relation indexRelation,
				   IndexInfo *indexInfo,
				   bool allow_sync,
				   IndexBuildCallback callback,
				   void *callback_state);
extern double IndexBuildHeapRangeScan(Relation heapRelation,
						Relation indexRelation,
						IndexInfo *indexInfo,
						bool allow_sync,
						bool anyvisible,
						BlockNumber start_blockno,
						BlockNumber end_blockno,
						IndexBuildCallback callback,
						void *callback_state);

extern void validate_index(Oid heapId, Oid indexId, Snapshot snapshot);

extern void index_set_state_flags(Oid indexId, IndexStateFlagsAction action);

extern void reindex_index(Oid indexId, bool skip_constraint_checks,
			  char relpersistence, int options);

/* Flag bits for reindex_relation(): */
#define REINDEX_REL_PROCESS_TOAST			0x01
#define REINDEX_REL_SUPPRESS_INDEX_USE		0x02
#define REINDEX_REL_CHECK_CONSTRAINTS		0x04
#define REINDEX_REL_FORCE_INDEXES_UNLOGGED	0x08
#define REINDEX_REL_FORCE_INDEXES_PERMANENT 0x10

extern bool reindex_relation(Oid relid, int flags, int options);

extern bool ReindexIsProcessingHeap(Oid heapOid);
extern bool ReindexIsProcessingIndex(Oid indexOid);
extern Oid	IndexGetRelation(Oid indexId, bool missing_ok);

#endif   /* INDEX_H */
