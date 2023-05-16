//---------------------------------------------------------------------------
//	@filename:
//		CPhysicalScan.cpp
//
//	@doc:
//		Implementation of base scan operator
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CPhysicalScan.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCastUtils.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpec.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecRandom.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/metadata/CName.h"
#include "duckdb/optimizer/cascade/metadata/CTableDescriptor.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::CPhysicalScan
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPhysicalScan::CPhysicalScan(CMemoryPool *mp, const CName *pnameAlias,
							 CTableDescriptor *ptabdesc,
							 CColRefArray *pdrgpcrOutput)
	: CPhysical(mp),
	  m_pnameAlias(pnameAlias),
	  m_ptabdesc(ptabdesc),
	  m_pdrgpcrOutput(pdrgpcrOutput),
	  m_pds(NULL),
	  m_pstatsBaseTable(NULL)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameAlias);
	GPOS_ASSERT(NULL != pdrgpcrOutput);

	if (ptabdesc->ConvertHashToRandom())
	{
		// Treating a hash distributed table as random during planning
		m_pds = GPOS_NEW(m_mp) CDistributionSpecRandom();
	}
	else
	{
		m_pds = CPhysical::PdsCompute(m_mp, ptabdesc, pdrgpcrOutput);
	}
	ComputeTableStats(m_mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::~CPhysicalScan
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CPhysicalScan::~CPhysicalScan()
{
	m_ptabdesc->Release();
	m_pdrgpcrOutput->Release();
	m_pds->Release();
	m_pstatsBaseTable->Release();
	GPOS_DELETE(m_pnameAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalScan::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalScan::FProvidesReqdCols(CExpressionHandle &,  // exprhdl
								 CColRefSet *pcrsRequired,
								 ULONG	// ulOptReq
) const
{
	GPOS_ASSERT(NULL != pcrsRequired);

	CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
	pcrs->Include(m_pdrgpcrOutput);

	BOOL result = pcrs->ContainsAll(pcrsRequired);
	pcrs->Release();

	return result;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalScan::EpetOrder(CExpressionHandle &,  // exprhdl
						 const CEnfdOrder *
#ifdef GPOS_DEBUG
							 peo
#endif	// GPOS_DEBUG
) const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->IsEmpty());

	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalScan::PdsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	BOOL fIndexOrBitmapScan =
		COperator::EopPhysicalIndexScan == Eopid() ||
		COperator::EopPhysicalBitmapTableScan == Eopid() ||
		COperator::EopPhysicalDynamicIndexScan == Eopid() ||
		COperator::EopPhysicalDynamicBitmapTableScan == Eopid();
	if (fIndexOrBitmapScan && CDistributionSpec::EdtHashed == m_pds->Edt() &&
		exprhdl.HasOuterRefs())
	{
		// If index conditions have outer references and the index relation is hashed,
		// check to see if we can derive an equivalent hashed distribution for the
		// outer references. For multi-distribution key tables with an index, it is
		// possible for a spec to have a column from both the inner and the outer
		// table. This is termed "incomplete" and added as an equivalent spec.
		//
		// For example, if we have foo (a, b) distributed by (a,b)
		//                         bar (c, d) distributed by (c, d)
		// with an index on idx_bar_d, if we have the query
		// 		select * from foo join bar on a = c and b = d,
		// it is possible to get a spec of [a, d].
		//
		// An incomplete spec is relevant only when we have an index join on a
		// multi-key distributed table. This is handled either by completing the
		// equivalent spec using Filter predicates above (see
		// CPhysicalFilter::PdsDerive()), or by discarding an incomplete spec at
		// the index join (see CPhysicalJoin::PdsDerive()).
		//
		// This way the equiv spec stays incomplete only as long as it needs to be.

		CExpression *pexprIndexPred = exprhdl.PexprScalarExactChild(
			0 /*child_index*/, true /*error_on_null_return*/);

		CDistributionSpecHashed *pdshashed =
			CDistributionSpecHashed::PdsConvert(m_pds);
		CDistributionSpecHashed *pdshashedEquiv =
			CDistributionSpecHashed::CompleteEquivSpec(mp, pdshashed,
													   pexprIndexPred);

		if (NULL != pdshashedEquiv)
		{
			CExpressionArray *pdrgpexprHashed = pdshashed->Pdrgpexpr();
			pdrgpexprHashed->AddRef();
			CDistributionSpecHashed *pdshashedResult = GPOS_NEW(mp)
				CDistributionSpecHashed(pdrgpexprHashed,
										pdshashed->FNullsColocated(),
										pdshashedEquiv);

			return pdshashedResult;
		}
	}

	m_pds->AddRef();

	return m_pds;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::PpimDeriveFromDynamicScan
//
//	@doc:
//		Derive partition index map from a dynamic scan operator
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPhysicalScan::PpimDeriveFromDynamicScan(CMemoryPool *mp, ULONG part_idx_id,
										 IMDId *rel_mdid,
										 CColRef2dArray *pdrgpdrgpcrPart,
										 ULONG ulSecondaryPartIndexId,
										 CPartConstraint *ppartcnstr,
										 CPartConstraint *ppartcnstrRel,
										 ULONG ulExpectedPropagators)
{
	CPartIndexMap *ppim = GPOS_NEW(mp) CPartIndexMap(mp);
	UlongToPartConstraintMap *ppartcnstrmap =
		GPOS_NEW(mp) UlongToPartConstraintMap(mp);

	(void) ppartcnstrmap->Insert(GPOS_NEW(mp) ULONG(ulSecondaryPartIndexId),
								 ppartcnstr);

	CPartKeysArray *pdrgppartkeys = GPOS_NEW(mp) CPartKeysArray(mp);
	pdrgppartkeys->Append(GPOS_NEW(mp) CPartKeys(pdrgpdrgpcrPart));

	ppim->Insert(part_idx_id, ppartcnstrmap, CPartIndexMap::EpimConsumer,
				 ulExpectedPropagators, rel_mdid, pdrgppartkeys, ppartcnstrRel);

	return ppim;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this
//		operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalScan::EpetDistribution(CExpressionHandle & /*exprhdl*/,
								const CEnfdDistribution *ped) const
{
	GPOS_ASSERT(NULL != ped);

	if (ped->FCompatible(m_pds))
	{
		// required distribution will be established by the operator
		return CEnfdProp::EpetUnnecessary;
	}

	// required distribution will be enforced on output
	return CEnfdProp::EpetRequired;
}



//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::ComputeTableStats
//
//	@doc:
//		Compute stats of underlying table
//
//---------------------------------------------------------------------------
void
CPhysicalScan::ComputeTableStats(CMemoryPool *mp)
{
	GPOS_ASSERT(NULL == m_pstatsBaseTable);

	CColRefSet *pcrsHist = GPOS_NEW(mp) CColRefSet(mp);
	CColRefSet *pcrsWidth = GPOS_NEW(mp) CColRefSet(mp, m_pdrgpcrOutput);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	m_pstatsBaseTable =
		md_accessor->Pstats(mp, m_ptabdesc->MDId(), pcrsHist, pcrsWidth);
	GPOS_ASSERT(NULL != m_pstatsBaseTable);

	pcrsHist->Release();
	pcrsWidth->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalScan *
CPhysicalScan::PopConvert(COperator *pop)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(CUtils::FPhysicalScan(pop));

	return dynamic_cast<CPhysicalScan *>(pop);
}