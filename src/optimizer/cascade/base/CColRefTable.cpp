//---------------------------------------------------------------------------
//	@filename:
//		CColRefTable.cpp
//
//	@doc:
//		Implementation of column reference class
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CColRefTable.h"

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CColRefTable::CColRefTable
//
//	@doc:
//		Ctor
//		takes ownership of string; verify string is properly formatted
//
//---------------------------------------------------------------------------
CColRefTable::CColRefTable(const CColumnDescriptor *pcoldesc, ULONG id,
						   const CName *pname, ULONG ulOpSource)
	: CColRef(pcoldesc->RetrieveType(), pcoldesc->TypeModifier(), id, pname),
	  m_iAttno(0),
	  m_ulSourceOpId(ulOpSource),
	  m_width(pcoldesc->Width())
{
	GPOS_ASSERT(NULL != pname);

	m_iAttno = pcoldesc->AttrNum();
	m_is_nullable = pcoldesc->IsNullable();
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefTable::CColRefTable
//
//	@doc:
//		Ctor
//		takes ownership of string; verify string is properly formatted
//
//---------------------------------------------------------------------------
CColRefTable::CColRefTable(const IMDType *pmdtype, INT type_modifier, INT attno,
						   BOOL is_nullable, ULONG id, const CName *pname,
						   ULONG ulOpSource, ULONG ulWidth)
	: CColRef(pmdtype, type_modifier, id, pname),
	  m_iAttno(attno),
	  m_is_nullable(is_nullable),
	  m_ulSourceOpId(ulOpSource),
	  m_width(ulWidth)
{
	GPOS_ASSERT(NULL != pname);
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefTable::~CColRefTable
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CColRefTable::~CColRefTable()
{
}