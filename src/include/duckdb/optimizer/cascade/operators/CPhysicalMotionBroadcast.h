//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalMotionBroadcast.h
//
//	@doc:
//		Physical Broadcast motion operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalMotionBroadcast_H
#define GPOPT_CPhysicalMotionBroadcast_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecReplicated.h"
#include "duckdb/optimizer/cascade/base/COrderSpec.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotion.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalMotionBroadcast
//
//	@doc:
//		Broadcast motion operator
//
//---------------------------------------------------------------------------
class CPhysicalMotionBroadcast : public CPhysicalMotion
{
private:
	// output distribution
	CDistributionSpecReplicated *m_pdsReplicated;

	// private copy ctor
	CPhysicalMotionBroadcast(const CPhysicalMotionBroadcast &);

public:
	// ctor
	explicit CPhysicalMotionBroadcast(CMemoryPool *mp);

	// dtor
	virtual ~CPhysicalMotionBroadcast();

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPhysicalMotionBroadcast;
	}

	virtual const CHAR *
	SzId() const
	{
		return "CPhysicalMotionBroadcast";
	}

	// output distribution accessor
	virtual CDistributionSpec *
	Pds() const
	{
		return m_pdsReplicated;
	}

	// match function
	virtual BOOL Matches(COperator *) const;

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
	static CPhysicalMotionBroadcast *PopConvert(COperator *pop);

};	// class CPhysicalMotionBroadcast

}  // namespace gpopt

#endif