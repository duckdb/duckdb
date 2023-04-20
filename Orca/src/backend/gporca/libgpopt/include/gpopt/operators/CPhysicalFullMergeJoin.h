//	Greenplum Database
//	Copyright (C) 2019 VMware, Inc. or its affiliates.

#ifndef GPOPT_CPhysicalFullMergeJoin_H
#define GPOPT_CPhysicalFullMergeJoin_H

#include "gpos/base.h"

#include "gpopt/operators/CPhysicalJoin.h"

namespace gpopt
{
class CPhysicalFullMergeJoin : public CPhysicalJoin
{
private:
	CExpressionArray *m_outer_merge_clauses;

	CExpressionArray *m_inner_merge_clauses;

public:
	CPhysicalFullMergeJoin(const CPhysicalFullMergeJoin &) = delete;

	// ctor
	explicit CPhysicalFullMergeJoin(CMemoryPool *mp,
									CExpressionArray *outer_merge_clauses,
									CExpressionArray *inner_merge_clauses,
									IMdIdArray *hash_opfamilies = NULL);

	// dtor
	~CPhysicalFullMergeJoin() override;

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPhysicalFullMergeJoin;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPhysicalFullMergeJoin";
	}

	// conversion function
	static CPhysicalFullMergeJoin *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(EopPhysicalFullMergeJoin == pop->Eopid());

		return dynamic_cast<CPhysicalFullMergeJoin *>(pop);
	}

	CDistributionSpec *PdsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CDistributionSpec *pdsRequired,
								   ULONG child_index,
								   CDrvdPropArray *pdrgpdpCtxt,
								   ULONG ulOptReq) const override;

	CEnfdDistribution *Ped(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   CReqdPropPlan *prppInput, ULONG child_index,
						   CDrvdPropArray *pdrgpdpCtxt,
						   ULONG ulDistrReq) override;

	COrderSpec *PosRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							COrderSpec *posInput, ULONG child_index,
							CDrvdPropArray *pdrgpdpCtxt,
							ULONG ulOptReq) const override;

	// compute required rewindability of the n-th child
	CRewindabilitySpec *PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
									CRewindabilitySpec *prsRequired,
									ULONG child_index,
									CDrvdPropArray *pdrgpdpCtxt,
									ULONG ulOptReq) const override;

	// return order property enforcing type for this operator
	CEnfdProp::EPropEnforcingType EpetOrder(
		CExpressionHandle &exprhdl, const CEnfdOrder *peo) const override;

	CEnfdDistribution::EDistributionMatching Edm(
		CReqdPropPlan *,   // prppInput
		ULONG,			   //child_index,
		CDrvdPropArray *,  // pdrgpdpCtxt,
		ULONG			   // ulOptReq
		) override;

	CDistributionSpec *PdsDerive(CMemoryPool *mp,
								 CExpressionHandle &exprhdl) const override;

};	// class CPhysicalFullMergeJoin

}  // namespace gpopt

#endif	// !GPOPT_CPhysicalFullMergeJoin_H

// EOF
