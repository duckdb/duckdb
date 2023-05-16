//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalAgg.cpp
//
//	@doc:
//		Implementation of basic aggregate operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalAgg.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecAny.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecHashed.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecRandom.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecSingleton.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecStrictSingleton.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::CPhysicalAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalAgg::CPhysicalAgg(CMemoryPool *mp, CColRefArray *colref_array, CColRefArray *pdrgpcrMinimal, COperator::EGbAggType egbaggtype, BOOL fGeneratesDuplicates, CColRefArray *pdrgpcrArgDQA, BOOL fMultiStage, BOOL isAggFromSplitDQA, CLogicalGbAgg::EAggStage aggStage, BOOL should_enforce_distribution)
	: CPhysical(mp), m_pdrgpcr(colref_array), m_egbaggtype(egbaggtype), m_isAggFromSplitDQA(isAggFromSplitDQA), m_aggStage(aggStage), m_pdrgpcrMinimal(NULL), m_fGeneratesDuplicates(fGeneratesDuplicates), m_pdrgpcrArgDQA(pdrgpcrArgDQA), m_fMultiStage(fMultiStage), m_should_enforce_distribution(should_enforce_distribution)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(COperator::EgbaggtypeSentinel > egbaggtype);
	GPOS_ASSERT_IMP(EgbaggtypeGlobal != egbaggtype, fMultiStage);

	ULONG ulDistrReqs = 1;
	if (pdrgpcrMinimal == NULL || 0 == pdrgpcrMinimal->Size())
	{
		colref_array->AddRef();
		m_pdrgpcrMinimal = colref_array;
	}
	else
	{
		pdrgpcrMinimal->AddRef();
		m_pdrgpcrMinimal = pdrgpcrMinimal;
	}

	if (COperator::EgbaggtypeLocal == egbaggtype)
	{
		// If the local aggregate has no distinct columns we generate
		// two optimization requests for its children:
		// (1) Any distribution requirement
		//
		// (2)	Random distribution requirement; this is needed to alleviate
		//		possible data skew

		ulDistrReqs = 2;
		if (pdrgpcrArgDQA != NULL && 0 != pdrgpcrArgDQA->Size())
		{
			// If the local aggregate has distinct columns we generate
			// two optimization requests for its children:
			// (1) hash distribution on the distinct columns only
			// (2) hash distribution on the grouping and distinct
			//     columns (only if the grouping columns are not empty)
			if (0 == m_pdrgpcr->Size())
			{
				ulDistrReqs = 1;
			}
		}
	}
	else if (COperator::EgbaggtypeIntermediate == egbaggtype)
	{
		GPOS_ASSERT(NULL != pdrgpcrArgDQA);
		GPOS_ASSERT(pdrgpcrArgDQA->Size() <= colref_array->Size());
		// Intermediate Agg generates two optimization requests for its children:
		// (1) Hash distribution on the group by columns + distinct column
		// (2) Hash distribution on the group by columns

		ulDistrReqs = 2;

		if (pdrgpcrArgDQA->Size() == colref_array->Size() ||
			GPOS_FTRACE(EopttraceForceAggSkewAvoidance))
		{
			// scalar aggregates so we only request the first case
			ulDistrReqs = 1;
		}
	}
	else if (COperator::EgbaggtypeGlobal == egbaggtype)
	{
		// Global Agg generates two optimization requests for its children:
		// (1) Singleton distribution, if child has volatile functions
		// (2) Hash distribution on the group by columns
		ulDistrReqs = 2;
	}

	SetDistrRequests(ulDistrReqs);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::~CPhysicalAgg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalAgg::~CPhysicalAgg()
{
	m_pdrgpcr->Release();
	m_pdrgpcrMinimal->Release();
	CRefCount::SafeRelease(m_pdrgpcrArgDQA);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalAgg::PcrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   CColRefSet *pcrsRequired, ULONG child_index,
						   CDrvdPropArray *,  // pdrgpdpCtxt
						   ULONG			  // ulOptReq
)
{
	return PcrsRequiredAgg(mp, exprhdl, pcrsRequired, child_index, m_pdrgpcr);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PcrsRequiredAgg
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalAgg::PcrsRequiredAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							  CColRefSet *pcrsRequired, ULONG child_index,
							  CColRefArray *pdrgpcrGrp)
{
	GPOS_ASSERT(NULL != pdrgpcrGrp);
	GPOS_ASSERT(
		0 == child_index &&
		"Required properties can only be computed on the relational child");

	CColRefSet *pcrs = GPOS_NEW(mp) CColRefSet(mp);

	// include grouping columns
	pcrs->Include(pdrgpcrGrp);
	pcrs->Union(pcrsRequired);

	CColRefSet *pcrsOutput =
		PcrsChildReqd(mp, exprhdl, pcrs, child_index, 1 /*ulScalarIndex*/);
	pcrs->Release();

	return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsRequiredAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
							 CDistributionSpec *pdsInput, ULONG child_index,
							 ULONG ulOptReq, CColRefArray *pdrgpcgGrp,
							 CColRefArray *pdrgpcrGrpMinimal) const
{
	GPOS_ASSERT(0 == child_index);

	if (FGlobal())
	{
		return PdsRequiredGlobalAgg(mp, exprhdl, pdsInput, child_index,
									pdrgpcgGrp, pdrgpcrGrpMinimal, ulOptReq);
	}

	if (COperator::EgbaggtypeIntermediate == m_egbaggtype)
	{
		return PdsRequiredIntermediateAgg(mp, ulOptReq);
	}

	// if expression has to execute on a single host then we need a gather
	if (exprhdl.NeedsSingletonExecution())
	{
		return PdsRequireSingleton(mp, exprhdl, pdsInput, child_index);
	}

	if (COperator::EgbaggtypeLocal == m_egbaggtype && m_pdrgpcrArgDQA != NULL &&
		0 != m_pdrgpcrArgDQA->Size())
	{
		if (ulOptReq == 0)
		{
			return PdsMaximalHashed(mp, m_pdrgpcrArgDQA);
		}
		else
		{
			GPOS_ASSERT(1 == ulOptReq);
			GPOS_ASSERT(0 < m_pdrgpcr->Size());
			CColRefArray *grpAndDistinctCols = GPOS_NEW(mp) CColRefArray(mp);
			grpAndDistinctCols->AppendArray(m_pdrgpcr);
			grpAndDistinctCols->AppendArray(m_pdrgpcrArgDQA);
			CDistributionSpec *pdsSpec =
				PdsMaximalHashed(mp, grpAndDistinctCols);
			grpAndDistinctCols->Release();
			return pdsSpec;
		}
	}

	GPOS_ASSERT(0 == ulOptReq || 1 == ulOptReq);

	if (0 == ulOptReq)
	{
		return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
	}

	// we randomly distribute the input for skew-elimination
	return GPOS_NEW(mp) CDistributionSpecRandom();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsMaximalHashed
//
//	@doc:
//		Compute a maximal hashed distribution using the given columns,
//		if no such distribution can be created, return a Singleton distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsMaximalHashed(CMemoryPool *mp, CColRefArray *colref_array)
{
	GPOS_ASSERT(NULL != colref_array);

	CDistributionSpecHashed *pdshashedMaximal =
		CDistributionSpecHashed::PdshashedMaximal(
			mp, colref_array, true /*fNullsColocated*/
		);
	if (NULL != pdshashedMaximal)
	{
		return pdshashedMaximal;
	}

	// otherwise, require a singleton explicitly
	return GPOS_NEW(mp) CDistributionSpecSingleton();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsRequiredGlobalAgg
//
//	@doc:
//		Compute required distribution of the n-th child of a global agg
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsRequiredGlobalAgg(CMemoryPool *mp, CExpressionHandle &exprhdl,
								   CDistributionSpec *pdsInput,
								   ULONG child_index, CColRefArray *pdrgpcrGrp,
								   CColRefArray *pdrgpcrGrpMinimal,
								   ULONG ulOptReq) const
{
	GPOS_ASSERT(FGlobal());
	GPOS_ASSERT(2 > ulOptReq);

	// TODO:  - Mar 19, 2012; Cleanup: move this check to the caller
	if (exprhdl.HasOuterRefs())
	{
		return PdsPassThru(mp, exprhdl, pdsInput, child_index);
	}

	if (0 == pdrgpcrGrp->Size())
	{
		if (CDistributionSpec::EdtSingleton == pdsInput->Edt())
		{
			// pass through input distribution if it is a singleton
			pdsInput->AddRef();
			return pdsInput;
		}

		// otherwise, require a singleton explicitly
		return GPOS_NEW(mp) CDistributionSpecSingleton();
	}

	if (0 == ulOptReq && (IMDFunction::EfsVolatile ==
						  exprhdl.DeriveFunctionProperties(0)->Efs()))
	{
		// request a singleton distribution if child has volatile functions
		return GPOS_NEW(mp) CDistributionSpecSingleton();
	}

	// if there are grouping columns, require a hash distribution explicitly
	return PdsMaximalHashed(mp, pdrgpcrGrpMinimal);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsRequiredIntermediateAgg
//
//	@doc:
//		Compute required distribution of the n-th child of an intermediate
//		aggregate operator
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsRequiredIntermediateAgg(CMemoryPool *mp, ULONG ulOptReq) const
{
	GPOS_ASSERT(COperator::EgbaggtypeIntermediate == m_egbaggtype);

	if (0 == ulOptReq)
	{
		return PdsMaximalHashed(mp, m_pdrgpcr);
	}

	CColRefArray *colref_array = GPOS_NEW(mp) CColRefArray(mp);
	const ULONG length = m_pdrgpcr->Size() - m_pdrgpcrArgDQA->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CColRef *colref = (*m_pdrgpcr)[ul];
		colref_array->Append(colref);
	}

	CDistributionSpec *pds = PdsMaximalHashed(mp, colref_array);
	colref_array->Release();

	return pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalAgg::PrsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
						  CRewindabilitySpec *prsRequired, ULONG child_index,
						  CDrvdPropArray *,	 // pdrgpdpCtxt
						  ULONG				 // ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);

	return PrsPassThru(mp, exprhdl, prsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalAgg::PppsRequired(CMemoryPool *mp, CExpressionHandle &exprhdl,
						   CPartitionPropagationSpec *pppsRequired,
						   ULONG
#ifdef GPOS_DEBUG
							   child_index
#endif
						   ,
						   CDrvdPropArray *,  //pdrgpdpCtxt,
						   ULONG			  //ulOptReq
)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(NULL != pppsRequired);

	return CPhysical::PppsRequiredPushThruUnresolvedUnary(
		mp, exprhdl, pppsRequired, CPhysical::EppcAllowed, NULL);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalAgg::PcteRequired(CMemoryPool *,		 //mp,
						   CExpressionHandle &,	 //exprhdl,
						   CCTEReq *pcter,
						   ULONG
#ifdef GPOS_DEBUG
							   child_index
#endif
						   ,
						   CDrvdPropArray *,  //pdrgpdpCtxt,
						   ULONG			  //ulOptReq
) const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalAgg::FProvidesReqdCols(CExpressionHandle &exprhdl,
								CColRefSet *pcrsRequired,
								ULONG  // ulOptReq
) const
{
	GPOS_ASSERT(NULL != pcrsRequired);
	GPOS_ASSERT(2 == exprhdl.Arity());

	CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);

	// include grouping columns
	pcrs->Include(PdrgpcrGroupingCols());

	// include defined columns by scalar child
	pcrs->Union(exprhdl.DeriveDefinedColumns(1));
	BOOL fProvidesCols = pcrs->ContainsAll(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalAgg::PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	CDistributionSpec *pds = exprhdl.Pdpplan(0 /*child_index*/)->Pds();

	if (CDistributionSpec::EdtUniversal == pds->Edt() &&
		IMDFunction::EfsVolatile ==
			exprhdl.DeriveScalarFunctionProperties(1)->Efs())
	{
		return GPOS_NEW(mp) CDistributionSpecStrictSingleton(
			CDistributionSpecSingleton::EstMaster);
	}

	pds->AddRef();
	return pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalAgg::PrsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	return PrsDerivePassThruOuter(mp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::HashValue
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalAgg::HashValue() const
{
	ULONG ulHash = COperator::HashValue();
	const ULONG arity = m_pdrgpcr->Size();
	ULONG ulGbaggtype = (ULONG) m_egbaggtype;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CColRef *colref = (*m_pdrgpcr)[ul];
		ulHash = gpos::CombineHashes(ulHash, gpos::HashPtr<CColRef>(colref));
	}

	ulHash = gpos::CombineHashes(ulHash, gpos::HashValue<ULONG>(&ulGbaggtype));

	return gpos::CombineHashes(ulHash,
							   gpos::HashValue<BOOL>(&m_fGeneratesDuplicates));
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::Matches
//
//	@doc:
//		Match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalAgg::Matches(COperator *pop) const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CPhysicalAgg *popAgg = reinterpret_cast<CPhysicalAgg *>(pop);

	if (FGeneratesDuplicates() != popAgg->FGeneratesDuplicates())
	{
		return false;
	}

	if (popAgg->Egbaggtype() == m_egbaggtype &&
		m_pdrgpcr->Equals(popAgg->m_pdrgpcr))
	{
		if (CColRef::Equals(m_pdrgpcrMinimal, popAgg->m_pdrgpcrMinimal))
		{
			return (m_pdrgpcrArgDQA == NULL || 0 == m_pdrgpcrArgDQA->Size()) ||
				   CColRef::Equals(m_pdrgpcrArgDQA, popAgg->PdrgpcrArgDQA());
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalAgg::EpetDistribution(CExpressionHandle &exprhdl,
							   const CEnfdDistribution *ped) const
{
	GPOS_ASSERT(NULL != ped);

	// get distribution delivered by the aggregate node
	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();

	if (ped->FCompatible(pds))
	{
		if (COperator::EgbaggtypeLocal != Egbaggtype() ||
			!m_should_enforce_distribution)
		{
			return CEnfdProp::EpetUnnecessary;
		}

		// prohibit the plan if local aggregate already delivers the enforced
		// distribution, since otherwise we would create two aggregates with
		// no intermediate motion operators
		return CEnfdProp::EpetProhibited;
	}

	// required distribution will be enforced on Agg's output
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalAgg::EpetRewindability(CExpressionHandle &exprhdl,
								const CEnfdRewindability *per) const
{
	// get rewindability delivered by the Agg node
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		// required rewindability is already provided
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetRequired;
}

BOOL
CPhysicalAgg::IsTwoStageScalarDQA() const
{
	return (m_aggStage == CLogicalGbAgg::EasTwoStageScalarDQA);
}

BOOL
CPhysicalAgg::IsThreeStageScalarDQA() const
{
	return (m_aggStage == CLogicalGbAgg::EasThreeStageScalarDQA);
}

BOOL
CPhysicalAgg::IsAggFromSplitDQA() const
{
	return m_isAggFromSplitDQA;
}
//---------------------------------------------------------------------------
//	@function:
//		CPhysicalAgg::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalAgg::OsPrint(IOstream &os) const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << "( ";
	CLogicalGbAgg::OsPrintGbAggType(os, m_egbaggtype);
	if (m_fMultiStage)
	{
		os << ", multi-stage";
	}
	os << " )";


	os << " Grp Cols: [";

	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os << "]"
	   << ", Minimal Grp Cols:[";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrMinimal);
	os << "]";

	if (COperator::EgbaggtypeIntermediate == m_egbaggtype)
	{
		os << ", Distinct Cols:[";
		CUtils::OsPrintDrgPcr(os, m_pdrgpcrArgDQA);
		os << "]";
	}
	os << ", Generates Duplicates :[ " << FGeneratesDuplicates() << " ] ";

	// note: 2-stage Scalar DQA and 3-stage scalar DQA are created by CXformSplitDQA only
	if (IsTwoStageScalarDQA())
	{
		os << ", m_aggStage :[ Two Stage Scalar DQA ] ";
	}

	if (IsThreeStageScalarDQA())
	{
		os << ", m_aggStage :[ Three Stage Scalar DQA ] ";
	}

	return os;
}