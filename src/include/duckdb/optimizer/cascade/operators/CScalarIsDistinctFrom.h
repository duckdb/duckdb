//---------------------------------------------------------------------------
//	@filename:
//		CScalarIsDistinctFrom.h
//
//	@doc:
//		Is distinct from operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CScalarIsDistinctFrom_H
#define GPOPT_CScalarIsDistinctFrom_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/operators/CScalarCmp.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CScalarIsDistinctFrom
//
//	@doc:
//		Is distinct from operator
//
//---------------------------------------------------------------------------
class CScalarIsDistinctFrom : public CScalarCmp
{
private:
	// private copy ctor
	CScalarIsDistinctFrom(const CScalarIsDistinctFrom &);

public:
	// ctor
	CScalarIsDistinctFrom(CMemoryPool *mp, IMDId *mdid_op,
						  const CWStringConst *pstrOp)
		: CScalarCmp(mp, mdid_op, pstrOp, IMDType::EcmptIDF)
	{
		GPOS_ASSERT(mdid_op->IsValid());
	}

	// dtor
	virtual ~CScalarIsDistinctFrom()
	{
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopScalarIsDistinctFrom;
	}

	// boolean expression evaluation
	virtual EBoolEvalResult Eber(ULongPtrArray *pdrgpulChildren) const;

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CScalarIsDistinctFrom";
	}

	virtual BOOL Matches(COperator *pop) const;

	// conversion function
	static CScalarIsDistinctFrom *PopConvert(COperator *pop);

	// get commuted scalar IDF operator
	virtual CScalarIsDistinctFrom *PopCommutedOp(CMemoryPool *mp,
												 COperator *pop);

};	// class CScalarIsDistinctFrom

}  // namespace gpopt

#endif