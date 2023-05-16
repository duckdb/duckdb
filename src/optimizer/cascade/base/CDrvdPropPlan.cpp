//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropPlan.cpp
//
//	@doc:
//		Derived plan properties
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CCTEMap.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base/CPartIndexMap.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CPhysical.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalCTEConsumer.h"
#include "duckdb/optimizer/cascade/operators/CScalar.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::CDrvdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::CDrvdPropPlan()
	: m_pos(NULL), m_pds(NULL), m_prs(NULL), m_ppim(NULL), m_ppfm(NULL), m_pcm(NULL)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::~CDrvdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDrvdPropPlan::~CDrvdPropPlan()
{
	CRefCount::SafeRelease(m_pos);
	CRefCount::SafeRelease(m_pds);
	CRefCount::SafeRelease(m_prs);
	CRefCount::SafeRelease(m_ppim);
	CRefCount::SafeRelease(m_ppfm);
	CRefCount::SafeRelease(m_pcm);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Pdpplan
//
//	@doc:
//		Short hand for conversion
//
//---------------------------------------------------------------------------
CDrvdPropPlan* CDrvdPropPlan::Pdpplan(CDrvdProp *pdp)
{
	GPOS_ASSERT(NULL != pdp);
	GPOS_ASSERT(EptPlan == pdp->Ept() && "This is not a plan properties container");
	return dynamic_cast<CDrvdPropPlan *>(pdp);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Derive
//
//	@doc:
//		Derive plan props
//
//---------------------------------------------------------------------------
void CDrvdPropPlan::Derive(CMemoryPool *mp, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdpctxt)
{
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	if (NULL != pdpctxt &&
		COperator::EopPhysicalCTEConsumer == popPhysical->Eopid())
	{
		CopyCTEProducerPlanProps(mp, pdpctxt, popPhysical);
	}
	else
	{
		// call property derivation functions on the operator
		m_pos = popPhysical->PosDerive(mp, exprhdl);
		m_pds = popPhysical->PdsDerive(mp, exprhdl);
		m_prs = popPhysical->PrsDerive(mp, exprhdl);
		m_ppim = popPhysical->PpimDerive(mp, exprhdl, pdpctxt);
		m_ppfm = popPhysical->PpfmDerive(mp, exprhdl);

		GPOS_ASSERT(NULL != m_ppim);
		GPOS_ASSERT(CDistributionSpec::EdtAny != m_pds->Edt() && "CDistributionAny is a require-only, cannot be derived");
	}
	m_pcm = popPhysical->PcmDerive(mp, exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::CopyCTEProducerPlanProps
//
//	@doc:
//		Copy CTE producer plan properties from given context to current object
//
//---------------------------------------------------------------------------
void CDrvdPropPlan::CopyCTEProducerPlanProps(CMemoryPool *mp, CDrvdPropCtxt *pdpctxt, COperator *pop)
{
	CDrvdPropCtxtPlan *pdpctxtplan = CDrvdPropCtxtPlan::PdpctxtplanConvert(pdpctxt);
	CPhysicalCTEConsumer *popCTEConsumer = CPhysicalCTEConsumer::PopConvert(pop);
	ULONG ulCTEId = popCTEConsumer->UlCTEId();
	UlongToColRefMap *colref_mapping = popCTEConsumer->Phmulcr();
	CDrvdPropPlan *pdpplan = pdpctxtplan->PdpplanCTEProducer(ulCTEId);
	if (NULL != pdpplan)
	{
		// copy producer plan properties after remapping columns
		m_pos = pdpplan->Pos()->PosCopyWithRemappedColumns(mp, colref_mapping, true /*must_exist*/);
		m_pds = pdpplan->Pds()->PdsCopyWithRemappedColumns(mp, colref_mapping, true /*must_exist*/);

		// rewindability and partition filter map do not need column remapping,
		// we add-ref producer's properties directly
		pdpplan->Prs()->AddRef();
		m_prs = pdpplan->Prs();

		pdpplan->Ppfm()->AddRef();
		m_ppfm = pdpplan->Ppfm();

		// no need to copy the part index map. return an empty one. This is to
		// distinguish between a CTE consumer and the inlined expression
		m_ppim = GPOS_NEW(mp) CPartIndexMap(mp);

		GPOS_ASSERT(CDistributionSpec::EdtAny != m_pds->Edt() && "CDistributionAny is a require-only, cannot be derived");
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::FSatisfies
//
//	@doc:
//		Check for satisfying required properties
//
//---------------------------------------------------------------------------
BOOL CDrvdPropPlan::FSatisfies(const CReqdPropPlan *prpp) const
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != prpp->Peo());
	GPOS_ASSERT(NULL != prpp->Ped());
	GPOS_ASSERT(NULL != prpp->Per());
	GPOS_ASSERT(NULL != prpp->Pepp());
	GPOS_ASSERT(NULL != prpp->Pcter());

	return m_pos->FSatisfies(prpp->Peo()->PosRequired()) && m_pds->FSatisfies(prpp->Ped()->PdsRequired()) && m_prs->FSatisfies(prpp->Per()->PrsRequired()) && m_ppim->FSatisfies(prpp->Pepp()->PppsRequired()) && m_pcm->FSatisfies(prpp->Pcter());
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG CDrvdPropPlan::HashValue() const
{
	ULONG ulHash = gpos::CombineHashes(m_pos->HashValue(), m_pds->HashValue());
	ulHash = gpos::CombineHashes(ulHash, m_prs->HashValue());
	ulHash = gpos::CombineHashes(ulHash, m_ppim->HashValue());
	ulHash = gpos::CombineHashes(ulHash, m_pcm->HashValue());

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
ULONG
CDrvdPropPlan::Equals(const CDrvdPropPlan *pdpplan) const
{
	return m_pos->Matches(pdpplan->Pos()) && m_pds->Equals(pdpplan->Pds()) && m_prs->Matches(pdpplan->Prs()) && m_ppim->Equals(pdpplan->Ppim()) && m_pcm->Equals(pdpplan->GetCostModel());
}

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropPlan::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDrvdPropPlan::OsPrint(IOstream &os) const
{
	os << "Drvd Plan Props (" << "ORD: " << (*m_pos) << ", DIST: " << (*m_pds) << ", REWIND: " << (*m_prs) << ")" << ", Part-Index Map: [" << *m_ppim << "]";
	os << ", Part Filter Map: ";
	m_ppfm->OsPrint(os);
	os << ", CTE Map: [" << *m_pcm << "]";
	return os;
}