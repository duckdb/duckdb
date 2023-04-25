//---------------------------------------------------------------------------
//	@filename:
//		CColumnFactory.h
//
//	@doc:
//		Column reference management; one instance per optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_CColumnFactory_H
#define GPOPT_CColumnFactory_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"
#include "duckdb/optimizer/cascade/common/CSyncHashtable.h"

#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/metadata/CColumnDescriptor.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

namespace gpopt
{
class CExpression;

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CColumnFactory
//
//	@doc:
//		Singleton factory class used to generate and manage CColRefs in ORCA.
//		The created CColRef objects are maintained in a hash table keyed by
//		Column ID.  CColumnFactory provides various overloaded PcrCreate()
//		methods to create CColRef and a LookupColRef() method to probe the hash
//		table.
//		NB: The class also owns the memory pool in which CColRefs are
//		allocated.
//
//---------------------------------------------------------------------------
class CColumnFactory
{
private:
	// MTS memory pool
	CMemoryPool *m_mp;

	// mapping between column id of computed column and a set of used column references
	ColRefToColRefSetMap *m_phmcrcrs;

	// id counter
	ULONG m_aul;

	// hash table
	CSyncHashtable<CColRef, ULONG> m_sht;

	// private copy ctor
	CColumnFactory(const CColumnFactory &);

	// implementation of factory methods
	CColRef *PcrCreate(const IMDType *pmdtype, INT type_modifier, ULONG id,
					   const CName &name);
	CColRef *PcrCreate(const CColumnDescriptor *pcoldesc, ULONG id,
					   const CName &name, ULONG ulOpSource,
					   BOOL mark_as_used = true, IMDId *mdid_table = NULL);

public:
	// ctor
	CColumnFactory();

	// dtor
	~CColumnFactory();

	// initialize the hash map between computed column and used columns
	void Initialize();

	// create a column reference given only its type and type modifier, used for computed columns
	CColRef *PcrCreate(const IMDType *pmdtype, INT type_modifier);

	// create column reference given its type, type modifier, and name
	CColRef *PcrCreate(const IMDType *pmdtype, INT type_modifier,
					   const CName &name);

	// create a column reference given its descriptor and name
	CColRef *PcrCreate(const CColumnDescriptor *pcoldescr, const CName &name,
					   ULONG ulOpSource, BOOL mark_as_used, IMDId *mdid_table);

	// create a column reference given its type, attno, nullability and name
	CColRef *PcrCreate(const IMDType *pmdtype, INT type_modifier,
					   IMDId *mdid_table, INT attno, BOOL is_nullable, ULONG id,
					   const CName &name, ULONG ulOpSource,
					   ULONG ulWidth = gpos::ulong_max);

	// create a column reference with the same type as passed column reference
	CColRef *
	PcrCreate(const CColRef *colref)
	{
		return PcrCreate(colref->RetrieveType(), colref->TypeModifier());
	}

	// add mapping between computed column to its used columns
	void AddComputedToUsedColsMap(CExpression *pexpr);

	// lookup the set of used column references (if any) based on id of computed column
	const CColRefSet *PcrsUsedInComputedCol(const CColRef *pcrComputedCol);

	// create a copy of the given colref
	CColRef *PcrCopy(const CColRef *colref);

	// lookup by id
	CColRef *LookupColRef(ULONG id);

	// destructor
	void Destroy(CColRef *);

};	// class CColumnFactory
}  // namespace gpopt

#endif
