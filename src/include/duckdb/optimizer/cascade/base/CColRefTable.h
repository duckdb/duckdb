//---------------------------------------------------------------------------
//	@filename:
//		CColRefTable.h
//
//	@doc:
//		Column reference implementation for base table columns
//---------------------------------------------------------------------------
#ifndef GPOS_CColRefTable_H
#define GPOS_CColRefTable_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CList.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/metadata/CColumnDescriptor.h"
#include "duckdb/optimizer/cascade/metadata/CName.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CColRefTable
//
//	@doc:
//		Column reference for base table columns
//
//---------------------------------------------------------------------------
class CColRefTable : public CColRef
{
private:
	// private copy ctor
	CColRefTable(const CColRefTable &);

	// attno from catalog
	INT m_iAttno;

	// does column allow null values
	BOOL m_is_nullable;

	// id of the operator which is the source of this column reference
	// not owned
	ULONG m_ulSourceOpId;

	// width of the column, for instance  char(10) column has width 10
	ULONG m_width;

public:
	// ctors
	CColRefTable(const CColumnDescriptor *pcd, ULONG id, const CName *pname,
				 ULONG ulOpSource);

	CColRefTable(const IMDType *pmdtype, INT type_modifier, INT attno,
				 BOOL is_nullable, ULONG id, const CName *pname,
				 ULONG ulOpSource, ULONG ulWidth = gpos::ulong_max);

	// dtor
	virtual ~CColRefTable();

	// accessor of column reference type
	virtual CColRef::Ecolreftype
	Ecrt() const
	{
		return CColRef::EcrtTable;
	}

	// accessor of attribute number
	INT
	AttrNum() const
	{
		return m_iAttno;
	}

	// does column allow null values?
	BOOL
	IsNullable() const
	{
		return m_is_nullable;
	}

	// is column a system column?
	BOOL
	FSystemCol() const
	{
		// TODO-  04/13/2012, make this check system independent
		// using MDAccessor
		return 0 >= m_iAttno;
	}

	// width of the column
	ULONG
	Width() const
	{
		return m_width;
	}

	// id of source operator
	ULONG
	UlSourceOpId() const
	{
		return m_ulSourceOpId;
	}

	// conversion
	static CColRefTable *
	PcrConvert(CColRef *cr)
	{
		GPOS_ASSERT(cr->Ecrt() == CColRef::EcrtTable);
		return dynamic_cast<CColRefTable *>(cr);
	}

};	// class CColRefTable
}  // namespace gpopt

#endif
