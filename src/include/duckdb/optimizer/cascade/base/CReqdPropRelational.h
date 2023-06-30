//---------------------------------------------------------------------------
//	@filename:
//		CReqdPropRelational.h
//
//	@doc:
//		Derived required relational properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CReqdPropRelational_H
#define GPOPT_CReqdPropRelational_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CReqdProp.h"

namespace gpopt
{
using namespace gpos;

// forward declaration

class CExpressionHandle;
class CColRefSet;

//---------------------------------------------------------------------------
//	@class:
//		CReqdPropRelational
//
//	@doc:
//		Required relational properties container.
//
//---------------------------------------------------------------------------
class CReqdPropRelational : public CReqdProp
{
private:
	// required stat columns
	CColRefSet *m_pcrsStat;

	// predicate on partition key
	CExpression *m_pexprPartPred;

	// private copy ctor
	CReqdPropRelational(const CReqdPropRelational &);

public:
	// default ctor
	CReqdPropRelational();

	// ctor
	explicit CReqdPropRelational(CColRefSet *pcrs);

	// ctor
	CReqdPropRelational(CColRefSet *pcrs, CExpression *pexprPartPred);

	// dtor
	virtual ~CReqdPropRelational();

	// type of properties
	virtual BOOL
	FRelational() const
	{
		GPOS_ASSERT(!FPlan());
		return true;
	}

	// stat columns accessor
	CColRefSet *
	PcrsStat() const
	{
		return m_pcrsStat;
	}

	// partition predicate accessor
	CExpression *
	PexprPartPred() const
	{
		return m_pexprPartPred;
	}

	// required properties computation function
	virtual void Compute(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 CReqdProp *prpInput, ULONG child_index,
						 CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

	// return difference from given properties
	CReqdPropRelational *PrprelDifference(CMemoryPool *mp,
										  CReqdPropRelational *prprel);

	// return true if property container is empty
	BOOL IsEmpty() const;

	// shorthand for conversion
	static CReqdPropRelational *GetReqdRelationalProps(CReqdProp *prp);

	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CReqdPropRelational

}  // namespace gpopt


#endif
