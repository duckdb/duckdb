//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalMotionRandom.h
//
//	@doc:
//		Physical Random motion operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalMotionRandom_H
#define GPOPT_CPhysicalMotionRandom_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CDistributionSpecRandom.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotion.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalMotionRandom
//
//	@doc:
//		Random motion operator
//
//---------------------------------------------------------------------------
class CPhysicalMotionRandom : public CPhysicalMotion
{
private:
	// distribution spec
	CDistributionSpecRandom *m_pdsRandom;

	// private copy ctor
	CPhysicalMotionRandom(const CPhysicalMotionRandom &);

public:
	// ctor
	CPhysicalMotionRandom(CMemoryPool *mp, CDistributionSpecRandom *pdsRandom);

	// dtor
	virtual ~CPhysicalMotionRandom();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalMotionRandom;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalMotionRandom";
	}

	// output distribution accessor
	virtual CDistributionSpec *
	Pds() const
	{
		return m_pdsRandom;
	}

	// is distribution duplicate sensitive
	BOOL
	IsDuplicateSensitive() const
	{
		return m_pdsRandom->IsDuplicateSensitive();
	}

	// match function
	virtual BOOL Matches(COperator *pop) const;

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required output columns of the n-th child
	virtual CColRefSet *PcrsRequired(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 CColRefSet *pcrsInput, ULONG child_index,
									 CDrvdPropArray *pdrgpdpCtxt,
									 ULONG ulOptReq);

	// compute required sort order of the n-th child
	virtual COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									COrderSpec *posInput, ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const;

	// check if required columns are included in output columns
	virtual BOOL FProvidesReqdCols(CExpressionHandle &exprhdl,
								   CColRefSet *pcrsRequired,
								   ULONG ulOptReq) const;

	//-------------------------------------------------------------------------------------
	// Derived Plan Properties
	//-------------------------------------------------------------------------------------

	// derive sort order
	virtual COrderSpec *PosDerive(CMemoryPool *mp,
								  CExpressionHandle &exprhdl) const;

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// print
	virtual IOstream &OsPrint(IOstream &) const;

	// conversion function
	static CPhysicalMotionRandom *PopConvert(COperator *pop);

};	// class CPhysicalMotionRandom

}  // namespace gpopt

#endif