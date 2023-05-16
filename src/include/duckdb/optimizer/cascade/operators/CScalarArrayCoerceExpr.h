//---------------------------------------------------------------------------
//	@filename:
//		CScalarArrayCoerceExpr.h
//
//	@doc:
//		Scalar Array Coerce Expr operator,
//		the operator will apply type casting for each element in this array
//		using the given element coercion function.
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarArrayCoerceExpr_H
#define GPOPT_CScalarArrayCoerceExpr_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/operators/CScalarCoerceBase.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarArrayCoerceExpr
//
//	@doc:
//		Scalar Array Coerce Expr operator
//
//---------------------------------------------------------------------------
class CScalarArrayCoerceExpr : public CScalarCoerceBase
{
private:
	// catalog MDId of the element function
	IMDId *m_pmdidElementFunc;

	// conversion semantics flag to pass to func
	BOOL m_is_explicit;

	// private copy ctor
	CScalarArrayCoerceExpr(const CScalarArrayCoerceExpr &);

public:
	// ctor
	CScalarArrayCoerceExpr(CMemoryPool *mp, IMDId *element_func,
						   IMDId *result_type_mdid, INT type_modifier,
						   BOOL is_explicit, ECoercionForm dxl_coerce_format,
						   INT location);

	// dtor
	virtual ~CScalarArrayCoerceExpr();

	// return metadata id of element coerce function
	IMDId *PmdidElementFunc() const;

	BOOL IsExplicit() const;

	virtual EOperatorId Eopid() const;

	// return a string for operator name
	virtual const CHAR *SzId() const;

	// match function
	virtual BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	virtual BOOL FInputOrderSensitive() const;

	// conversion function
	static CScalarArrayCoerceExpr *PopConvert(COperator *pop);

};	// class CScalarArrayCoerceExpr

}  // namespace gpopt

#endif