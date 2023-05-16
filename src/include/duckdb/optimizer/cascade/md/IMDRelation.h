//---------------------------------------------------------------------------
//	@filename:
//		IMDRelation.h
//
//	@doc:
//		Interface for relations in the metadata cache
//---------------------------------------------------------------------------
#ifndef GPMD_IMDRelation_H
#define GPMD_IMDRelation_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/CMDIndexInfo.h"
#include "duckdb/optimizer/cascade/md/IMDCacheObject.h"
#include "duckdb/optimizer/cascade/md/IMDColumn.h"
#include "duckdb/optimizer/cascade/md/IMDPartConstraint.h"
#include "duckdb/optimizer/cascade/statistics/IStatistics.h"

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDRelation
//
//	@doc:
//		Interface for relations in the metadata cache
//
//---------------------------------------------------------------------------
class IMDRelation : public IMDCacheObject
{
public:
	//-------------------------------------------------------------------
	//	@doc:
	//		Storage type of a relation
	//-------------------------------------------------------------------
	enum Erelstoragetype
	{
		ErelstorageHeap,
		ErelstorageAppendOnlyCols,
		ErelstorageAppendOnlyRows,
		ErelstorageAppendOnlyParquet,
		ErelstorageExternal,
		ErelstorageVirtual,
		ErelstorageSentinel
	};

	//-------------------------------------------------------------------
	//	@doc:
	//		Distribution policy of a relation
	//-------------------------------------------------------------------
	enum Ereldistrpolicy
	{
		EreldistrMasterOnly,
		EreldistrHash,
		EreldistrRandom,
		EreldistrReplicated,
		EreldistrSentinel
	};

	// Partition type of a partitioned relation
	enum Erelpartitiontype
	{
		ErelpartitionRange = 'r',
		ErelpartitionList = 'l'
	};

public:
	// object type
	virtual Emdtype
	MDType() const
	{
		return EmdtRel;
	}

	// is this a temp relation
	virtual BOOL IsTemporary() const = 0;

	// storage type (heap, appendonly, ...)
	virtual Erelstoragetype RetrieveRelStorageType() const = 0;

	// distribution policy (none, hash, random)
	virtual Ereldistrpolicy GetRelDistribution() const = 0;

	// number of columns
	virtual ULONG ColumnCount() const = 0;

	// width of a column with regards to the position
	virtual DOUBLE ColWidth(ULONG pos) const = 0;

	// does relation have dropped columns
	virtual BOOL HasDroppedColumns() const = 0;

	// number of non-dropped columns
	virtual ULONG NonDroppedColsCount() const = 0;

	// return the position of the given attribute position excluding dropped columns
	virtual ULONG NonDroppedColAt(ULONG pos) const = 0;

	// return the position of a column in the metadata object given the attribute number in the system catalog
	virtual ULONG GetPosFromAttno(INT attno) const = 0;

	// return the original positions of all the non-dropped columns
	virtual ULongPtrArray *NonDroppedColsArray() const = 0;

	// number of system columns
	virtual ULONG SystemColumnsCount() const = 0;

	// retrieve the column at the given position
	virtual const IMDColumn *GetMdCol(ULONG pos) const = 0;

	// number of key sets
	virtual ULONG KeySetCount() const = 0;

	// key set at given position
	virtual const ULongPtrArray *KeySetAt(ULONG pos) const = 0;

	// number of distribution columns
	virtual ULONG DistrColumnCount() const = 0;

	// retrieve the column at the given position in the distribution key for the relation
	virtual const IMDColumn *GetDistrColAt(ULONG pos) const = 0;

	// return true if a hash distributed table needs to be considered as random
	virtual BOOL ConvertHashToRandom() const = 0;

	// does this table have oids
	virtual BOOL HasOids() const = 0;

	// is this a partitioned table
	virtual BOOL IsPartitioned() const = 0;

	// number of partition columns
	virtual ULONG PartColumnCount() const = 0;

	// number of partitions
	virtual ULONG PartitionCount() const = 0;

	// retrieve the partition column at the given position
	virtual const IMDColumn *PartColAt(ULONG pos) const = 0;

	// retrieve list of partition types
	virtual CharPtrArray *GetPartitionTypes() const = 0;

	// retrieve the partition type of the given partition level
	virtual CHAR PartTypeAtLevel(ULONG pos) const = 0;

	// number of indices
	virtual ULONG IndexCount() const = 0;

	// number of triggers
	virtual ULONG TriggerCount() const = 0;

	// retrieve the id of the metadata cache index at the given position
	virtual IMDId *IndexMDidAt(ULONG pos) const = 0;

	// check if index is partial given its mdid
	virtual BOOL IsPartialIndex(IMDId *mdid) const;

	// retrieve the id of the metadata cache trigger at the given position
	virtual IMDId *TriggerMDidAt(ULONG pos) const = 0;

	// number of check constraints
	virtual ULONG CheckConstraintCount() const = 0;

	// retrieve the id of the check constraint cache at the given position
	virtual IMDId *CheckConstraintMDidAt(ULONG pos) const = 0;

	// part constraint
	virtual IMDPartConstraint *MDPartConstraint() const = 0;

	// relation distribution policy as a string value
	static const CWStringConst *GetDistrPolicyStr(
		Ereldistrpolicy rel_distr_policy);

	// name of storage type
	static const CWStringConst *GetStorageTypeStr(
		IMDRelation::Erelstoragetype rel_storage_type);

	BOOL
	IsAORowOrColTable() const
	{
		Erelstoragetype st = RetrieveRelStorageType();
		return st == ErelstorageAppendOnlyCols ||
			   st == ErelstorageAppendOnlyRows;
	}
};

// common structure over relation and external relation metadata for index info
typedef CDynamicPtrArray<CMDIndexInfo, CleanupRelease> CMDIndexInfoArray;

}  // namespace gpmd

#endif
