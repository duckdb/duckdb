//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalNLJoin.h
//
//	@doc:
//		Base nested-loops join operator
//---------------------------------------------------------------------------
#ifndef GPOPT_CPhysicalNLJoin_H
#define GPOPT_CPhysicalNLJoin_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalJoin.h"

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPhysicalNLJoin
//
//	@doc:
//		Inner nested-loops join operator
//
//---------------------------------------------------------------------------
class CPhysicalNLJoin : public CPhysicalJoin
{
private:
	// private copy ctor
	CPhysicalNLJoin(const CPhysicalNLJoin &);

protected:
	// helper function for computing the required partition propagation
	// spec for the children of a nested loop join
	CPartitionPropagationSpec *PppsRequiredNLJoinChild(
		CMemoryPool *mp, CExpressionHandle &exprhdl,
		CPartitionPropagationSpec *pppsRequired, ULONG child_index,
		CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq);

public:
	// ctor
	explicit CPhysicalNLJoin(CMemoryPool *mp);

	// dtor
	virtual ~CPhysicalNLJoin();

	//-------------------------------------------------------------------------------------
	// Required Plan Properties
	//-------------------------------------------------------------------------------------

	// compute required sort order of the n-th child
	virtual COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									COrderSpec *posInput, ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const;

	// compute required rewindability of the n-th child
	virtual CRewindabilitySpec *PrsRequired(CMemoryPool *mp,
											CExpressionHandle &exprhdl,
											CRewindabilitySpec *prsRequired,
											ULONG child_index,
											CDrvdPropArray *pdrgpdpCtxt,
											ULONG ulOptReq) const;

	// compute required output columns of the n-th child
	virtual CColRefSet *PcrsRequired(CMemoryPool *mp,
									 CExpressionHandle &exprhdl,
									 CColRefSet *pcrsRequired,
									 ULONG child_index,
									 CDrvdPropArray *,	// pdrgpdpCtxt
									 ULONG				// ulOptReq
	);

	// compute required partition propagation of the n-th child
	virtual CPartitionPropagationSpec *
	PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
				 CPartitionPropagationSpec *pppsRequired, ULONG child_index,
				 CDrvdPropArray *pdrgpdpCtxt, ULONG ulOptReq)
	{
		GPOS_ASSERT(ulOptReq < UlPartPropagateRequests());

		return PppsRequiredNLJoinChild(mp, exprhdl, pppsRequired, child_index,
									   pdrgpdpCtxt, ulOptReq);
	}

	//-------------------------------------------------------------------------------------
	// Enforced Properties
	//-------------------------------------------------------------------------------------

	// return order property enforcing type for this operator
	virtual CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const;

	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------
	//-------------------------------------------------------------------------------------

	// return true if operator is a correlated NL Join
	virtual BOOL
	FCorrelated() const
	{
		return false;
	}

	// return required inner columns -- overloaded by correlated join children
	virtual CColRefArray *
	PdrgPcrInner() const
	{
		return NULL;
	}

	// conversion function
	static CPhysicalNLJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(CUtils::FNLJoin(pop));

		return dynamic_cast<CPhysicalNLJoin *>(pop);
	}


};	// class CPhysicalNLJoin

}  // namespace gpopt

#endif