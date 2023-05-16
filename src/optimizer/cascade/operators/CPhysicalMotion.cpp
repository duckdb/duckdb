//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalMotion.cpp
//
//	@doc:
//		Implementation of motion operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalMotion.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecAny.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecRandom.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/search/CMemo.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::FValidContext
//
//	@doc:
//		Check if optimization context is valid
//
//---------------------------------------------------------------------------
BOOL CPhysicalMotion::FValidContext(CMemoryPool *, COptimizationContext *poc, COptimizationContextArray *pdrgpocChild) const
{
	GPOS_ASSERT(NULL != pdrgpocChild);
	GPOS_ASSERT(1 == pdrgpocChild->Size());

	COptimizationContext *pocChild = (*pdrgpocChild)[0];
	CCostContext *pccBest = pocChild->PccBest();
	GPOS_ASSERT(NULL != pccBest);

	CDrvdPropPlan *pdpplanChild = pccBest->Pdpplan();
	if (pdpplanChild->Ppim()->FContainsUnresolved())
	{
		return false;
	}

	CEnfdDistribution *ped = poc->Prpp()->Ped();
	if (ped->FCompatible(this->Pds()) && ped->FCompatible(pdpplanChild->Pds()))
	{
		// required distribution is compatible with the distribution delivered by Motion and its child plan,
		// in this case, Motion is redundant since child plan delivers the required distribution
		return false;
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalMotion::PdsRequired(CMemoryPool *mp,
							 CExpressionHandle &,  // exprhdl
							 CDistributionSpec *pdsRequired,
							 ULONG
#ifdef GPOS_DEBUG
								 child_index
#endif	// GPOS_DEBUG
							 ,
							 CDrvdPropArray *,	// pdrgpdpCtxt
							 ULONG				// ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	// if the required distribution is EdtStrictRandom, it indicates
	// that this motion operator was enforced into the same
	// group as that of CPhysicalComputeScalar just below CPhysicalDML(Insert)
	// to enforce CDistributionSpecStrictRandom. So, it should request
	// CDistributionSpecRandom from its child i.e CPhysicalComputeScalar
	// which already delivers CDistributionSpecRandom. This will result in
	// a valid plan enforcing the required specs.
	//
	// for the query: explain insert into t1_random select * from t1_random;
	// below is the memo after optimization and physical plan
	//
	//	Group 5 (#GExprs: 3):
	//	0: CLogicalProject [ 0 4 ]
	//	1: CPhysicalComputeScalar [ 0 4 ]    ==> Derives CDistributionSpecRandom(false)
	//	2: CPhysicalMotionRandom [ 5 ]       ==> Enforced CDistributionSpecStrictRandom
	//	",
	//
	//	Group 4 ():
	//	0: CScalarProjectList [ 3 ]
	//	",
	//
	//	Group 3 ():
	//	0: CScalarProjectElement "ColRef_0008" (8) [ 2 ]
	//	",
	//
	//	Group 2 ():
	//	0: CScalarConst (1) [ ]
	//	",
	//
	//	ROOT
	//	Group 1 (#GExprs: 3):
	//	0: CLogicalInsert ("t1_random")
	//	1: CLogicalDML (Insert, "t1_random")
	//	2: CPhysicalDML (Insert, "t1_random")
	//
	//	Group 0 (#GExprs: 2):
	//	0: CLogicalGet "t1_random" ("t1_random")
	//	1: CPhysicalTableScan "t1_random" ("t1_random") ===> Derives CDistributionSpecRandom(false)
	//	",
	//
	//	Physical plan:
	//	+--CPhysicalDML (Insert, "t1_random"), Source Columns: ["a" (0)], Action: ("ColRef_0008" (8))
	//	   +--CPhysicalMotionRandom
	//	      +--CPhysicalComputeScalar
	//	      |--CPhysicalTableScan "t1_random"
	//	      +--CScalarProjectList
	//	         +--CScalarProjectElement "ColRef_0008"
	//	            +--CScalarConst (1)
	if (CDistributionSpec::EdtStrictRandom == pdsRequired->Edt())
	{
		GPOS_ASSERT(COperator::EopPhysicalMotionRandom == Eopid());
		GPOS_ASSERT(CDistributionSpec::EdtStrictRandom == Pds()->Edt());
		return GPOS_NEW(mp) CDistributionSpecRandom();
	}

	// any motion operator is distribution-establishing and does not require
	// child to deliver any specific distribution
	return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalMotion::PrsRequired(CMemoryPool *mp,
							 CExpressionHandle &,	// exprhdl
							 CRewindabilitySpec *,	// prsRequired
							 ULONG
#ifdef GPOS_DEBUG
								 child_index
#endif	// GPOS_DEBUG
							 ,
							 CDrvdPropArray *,	// pdrgpdpCtxt
							 ULONG				// ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	// A motion is a hard barrier for rewindability since it executes in a
	// different slice; and thus it cannot require any rewindability property
	// from its child
	return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone,
										   CRewindabilitySpec::EmhtNoMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalMotion::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  CPartitionPropagationSpec *pppsRequired,
							  ULONG
#ifdef GPOS_DEBUG
								  child_index
#endif	// GPOS_DEBUG
							  ,
							  CDrvdPropArray *,	 //pdrgpdpCtxt,
							  ULONG				 //ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(NULL != pppsRequired);

	CPartIndexMap *ppimReqd = pppsRequired->Ppim();
	CPartFilterMap *ppfmReqd = pppsRequired->Ppfm();

	ULongPtrArray *pdrgpul = ppimReqd->PdrgpulScanIds(mp);

	CPartIndexMap *ppimResult = GPOS_NEW(mp) CPartIndexMap(mp);
	CPartFilterMap *ppfmResult = GPOS_NEW(mp) CPartFilterMap(mp);

	/// get derived part consumers
	CPartInfo *ppartinfo = exprhdl.DerivePartitionInfo(0);

	const ULONG ulPartIndexSize = pdrgpul->Size();

	for (ULONG ul = 0; ul < ulPartIndexSize; ul++)
	{
		ULONG part_idx_id = *((*pdrgpul)[ul]);

		if (!ppartinfo->FContainsScanId(part_idx_id))
		{
			// part index id does not exist in child nodes: do not push it below
			// the motion
			continue;
		}

		ppimResult->AddRequiredPartPropagation(
			ppimReqd, part_idx_id, CPartIndexMap::EppraPreservePropagators);
		(void) ppfmResult->FCopyPartFilter(m_mp, part_idx_id, ppfmReqd, NULL);
	}

	pdrgpul->Release();

	return GPOS_NEW(mp) CPartitionPropagationSpec(ppimResult, ppfmResult);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalMotion::PcteRequired(CMemoryPool *,		//mp,
							  CExpressionHandle &,	//exprhdl,
							  CCTEReq *pcter,
							  ULONG
#ifdef GPOS_DEBUG
								  child_index
#endif
							  ,
							  CDrvdPropArray *,	 //pdrgpdpCtxt,
							  ULONG				 //ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalMotion::PdsDerive(CMemoryPool * /*mp*/, CExpressionHandle & /*exprhdl*/
) const
{
	CDistributionSpec *pds = Pds();
	pds->AddRef();

	return pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalMotion::PrsDerive(CMemoryPool *mp,
						   CExpressionHandle &	// exprhdl
) const
{
	// A motion does not preserve rewindability and is also not rescannable.
	return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNone,
										   CRewindabilitySpec::EmhtMotion);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::EpetDistribution
//
//	@doc:
//		Return distribution property enforcing type for this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotion::EpetDistribution(CExpressionHandle &,	// exprhdl
								  const CEnfdDistribution *ped) const
{
	GPOS_ASSERT(NULL != ped);

	if (ped->FCompatible(Pds()))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetProhibited;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::EpetRewindability
//
//	@doc:
//		Return rewindability property enforcing type for this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotion::EpetRewindability(CExpressionHandle &exprhdl,
								   const CEnfdRewindability *  // per
) const
{
	if (exprhdl.HasOuterRefs())
	{
		// motion has outer references: prohibit this plan
		// Note: this is a GPDB restriction as Motion operators are push-based
		return CEnfdProp::EpetProhibited;
	}

	// motion does not provide rewindability on its output
	return CEnfdProp::EpetRequired;
}