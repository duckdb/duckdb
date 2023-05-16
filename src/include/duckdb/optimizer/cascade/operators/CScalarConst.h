//---------------------------------------------------------------------------
//	@filename:
//		CScalarConst.h
//
//	@doc:
//		An operator class that wraps a scalar constant
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarConst_H
#define GPOPT_CScalarConst_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/CDrvdProp.h"
#include "duckdb/optimizer/cascade/operators/CScalar.h"
#include "duckdb/optimizer/cascade/base/IDatum.h"

namespace gpopt
{
using namespace gpos;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@class:
//		CScalarConst
//
//	@doc:
//		A wrapper operator for scalar constants
//
//---------------------------------------------------------------------------
class CScalarConst : public CScalar
{
private:
	// constant
	IDatum *m_pdatum;

	// private copy ctor
	CScalarConst(const CScalarConst &);

public:
	// ctor
	CScalarConst(CMemoryPool *mp, IDatum *datum);

	// dtor
	virtual ~CScalarConst();

	// identity accessor
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarConst;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CScalarConst";
	}

	// accessor of contained constant
	IDatum *
	GetDatum() const
	{
		return m_pdatum;
	}

	// operator specific hash function
	virtual ULONG HashValue() const;

	// match function
	virtual BOOL Matches(COperator *pop) const;

	// sensitivity to order of inputs
	virtual BOOL
	FInputOrderSensitive() const
	{
		return false;
	}

	// return a copy of the operator with remapped columns
	virtual COperator *
	PopCopyWithRemappedColumns(CMemoryPool *,		//mp,
							   UlongToColRefMap *,	//colref_mapping,
							   BOOL					//must_exist
	)
	{
		return PopCopyDefault();
	}

	// conversion function
	static CScalarConst *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(EopScalarConst == pop->Eopid());

		return reinterpret_cast<CScalarConst *>(pop);
	}

	// the type of the scalar expression
	virtual IMDId *MdidType() const;

	virtual INT TypeModifier() const;

	// boolean expression evaluation
	virtual EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const;

	// print
	virtual IOstream &OsPrint(IOstream &) const;

	// is the given expression a scalar cast of a constant
	static BOOL FCastedConst(CExpression *pexpr);

	// extract the constant from the given constant expression or a casted constant expression.
	// Else return NULL.
	static CScalarConst *PopExtractFromConstOrCastConst(CExpression *pexpr);

};	// class CScalarConst

}  // namespace gpopt

#endif
