//---------------------------------------------------------------------------
//	@filename:
//		CScalarCoerceBase.cpp
//
//	@doc:
//		Implementation of scalar coerce operator base class
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CScalarCoerceBase.h"
#include "duckdb/optimizer/cascade/base.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::CScalarCoerceBase
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCoerceBase::CScalarCoerceBase(CMemoryPool *mp, IMDId *mdid_type, INT type_modifier, ECoercionForm ecf, INT location)
	: CScalar(mp), m_result_type_mdid(mdid_type), m_type_modifier(type_modifier), m_ecf(ecf), m_location(location)
{
	GPOS_ASSERT(NULL != mdid_type);
	GPOS_ASSERT(mdid_type->IsValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::~CScalarCoerceBase
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarCoerceBase::~CScalarCoerceBase()
{
	m_result_type_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::MdidType
//
//	@doc:
//		Return type of the scalar expression
//
//---------------------------------------------------------------------------
IMDId* CScalarCoerceBase::MdidType() const
{
	return m_result_type_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::TypeModifier
//
//	@doc:
//		Return type modifier
//
//---------------------------------------------------------------------------
INT CScalarCoerceBase::TypeModifier() const
{
	return m_type_modifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::Ecf
//
//	@doc:
//		Return coercion form
//
//---------------------------------------------------------------------------
CScalar::ECoercionForm CScalarCoerceBase::Ecf() const
{
	return m_ecf;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::Location
//
//	@doc:
//		Return token location
//
//---------------------------------------------------------------------------
INT CScalarCoerceBase::Location() const
{
	return m_location;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::PopCopyWithRemappedColumns
//
//	@doc:
//		return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator* CScalarCoerceBase::PopCopyWithRemappedColumns(CMemoryPool* mp, UlongToColRefMap* colref_mapping, BOOL must_exist)
{
	return PopCopyDefault();
}