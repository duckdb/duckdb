//---------------------------------------------------------------------------
//	@filename:
//		CPattern.cpp
//
//	@doc:
//		Implementation of base class of pattern operators
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPattern.h"
#include "duckdb/optimizer/cascade/base.h"

namespace gpopt
{
using namespace duckdb;

//---------------------------------------------------------------------------
//	@function:
//		CPattern::PdpCreate
//
//	@doc:
//		Pattern operators cannot derive properties; the assembly of the
//		expression has to take care of this on a higher level
//
//---------------------------------------------------------------------------
CDrvdProp* CPattern::PdpCreate()
{
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPattern::PrpCreate
//
//	@doc:
//		Pattern operators cannot compute required properties; the assembly of the
//		expression has to take care of this on a higher level
//
//---------------------------------------------------------------------------
CReqdProp* CPattern::PrpCreate() const
{
	return nullptr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPattern::Matches
//
//	@doc:
//		match against an operator
//
//---------------------------------------------------------------------------
bool CPattern::Matches(Operator *pop)
{
	return logical_type == pop->logical_type && physical_type == pop->physical_type;
}

//---------------------------------------------------------------------------
//	@function:
//		CPattern::FInputOrderSensitive
//
//	@doc:
//		By default patterns are leaves; no need to call this function ever
//
//---------------------------------------------------------------------------
bool CPattern::FInputOrderSensitive()
{
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPattern::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
Operator* CPattern::PopCopyWithRemappedColumns(std::map<ULONG, ColumnBinding> colref_mapping, bool must_exist)
{
	return NULL;
}
}