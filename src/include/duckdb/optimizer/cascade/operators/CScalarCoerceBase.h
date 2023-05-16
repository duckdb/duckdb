//---------------------------------------------------------------------------
//	@filename:
//		CScalarCoerceBase.h
//
//	@doc:
//		Scalar coerce operator base class
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarCoerceBase_H
#define GPOPT_CScalarCoerceBase_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/operators/CScalar.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarCoerceBase
//
//	@doc:
//		Scalar coerce operator base class
//
//---------------------------------------------------------------------------
class CScalarCoerceBase : public CScalar
{
private:
	// catalog MDId of the result type
	IMDId *m_result_type_mdid;

	// output type modifier
	INT m_type_modifier;

	// coercion form
	ECoercionForm m_ecf;

	// location of token to be coerced
	INT m_location;

	// private copy ctor
	CScalarCoerceBase(const CScalarCoerceBase &);

public:
	// ctor
	CScalarCoerceBase(CMemoryPool *mp, IMDId *mdid_type, INT type_modifier,
					  ECoercionForm dxl_coerce_format, INT location);

	// dtor
	virtual ~CScalarCoerceBase();

	// the type of the scalar expression
	virtual IMDId *MdidType() const;

	// return type modifier
	INT TypeModifier() const;

	// return coercion form
	ECoercionForm Ecf() const;

	// return token location
	INT Location() const;

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

};	// class CScalarCoerceBase

}  // namespace gpopt

#endif