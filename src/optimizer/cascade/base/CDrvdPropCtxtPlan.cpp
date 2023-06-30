//---------------------------------------------------------------------------
//	@filename:
//		CDrvdPropCtxtPlan.cpp
//
//	@doc:
//		Derived plan properties context
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CDrvdPropCtxtPlan.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CCTEMap.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalPartitionSelector.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalScan.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalUnionAll.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::CDrvdPropCtxtPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDrvdPropCtxtPlan::CDrvdPropCtxtPlan(CMemoryPool *mp, BOOL fUpdateCTEMap)
	: CDrvdPropCtxt(mp),
	  m_phmulpdpCTEs(NULL),
	  m_ulExpectedPartitionSelectors(0),
	  m_fUpdateCTEMap(fUpdateCTEMap)
{
	m_phmulpdpCTEs = GPOS_NEW(m_mp) UlongToDrvdPropPlanMap(m_mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::~CDrvdPropCtxtPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDrvdPropCtxtPlan::~CDrvdPropCtxtPlan()
{
	m_phmulpdpCTEs->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::PdpctxtCopy
//
//	@doc:
//		Copy function
//
//---------------------------------------------------------------------------
CDrvdPropCtxt *
CDrvdPropCtxtPlan::PdpctxtCopy(CMemoryPool *mp) const
{
	CDrvdPropCtxtPlan *pdpctxtplan = GPOS_NEW(mp) CDrvdPropCtxtPlan(mp);
	pdpctxtplan->m_ulExpectedPartitionSelectors =
		m_ulExpectedPartitionSelectors;

	UlongToDrvdPropPlanMapIter hmulpdpiter(m_phmulpdpCTEs);
	while (hmulpdpiter.Advance())
	{
		ULONG id = *(hmulpdpiter.Key());
		CDrvdPropPlan *pdpplan =
			const_cast<CDrvdPropPlan *>(hmulpdpiter.Value());
		pdpplan->AddRef();
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif	// GPOS_DEBUG
			pdpctxtplan->m_phmulpdpCTEs->Insert(GPOS_NEW(m_mp) ULONG(id),
												pdpplan);
		GPOS_ASSERT(fInserted);
	}

	return pdpctxtplan;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::AddProps
//
//	@doc:
//		Add props to context
//
//---------------------------------------------------------------------------
void
CDrvdPropCtxtPlan::AddProps(CDrvdProp *pdp)
{
	if (CDrvdProp::EptPlan != pdp->Ept())
	{
		// passed property is not a plan property container
		return;
	}

	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan(pdp);

	ULONG ulProducerId = gpos::ulong_max;
	CDrvdPropPlan *pdpplanProducer =
		pdpplan->GetCostModel()->PdpplanProducer(&ulProducerId);
	if (NULL == pdpplanProducer)
	{
		return;
	}

	if (m_fUpdateCTEMap)
	{
		pdpplanProducer->AddRef();
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif	// GPOS_DEBUG
			m_phmulpdpCTEs->Insert(GPOS_NEW(m_mp) ULONG(ulProducerId),
								   pdpplanProducer);
		GPOS_ASSERT(fInserted);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDrvdPropCtxtPlan::OsPrint(IOstream &os) const
{
	// iterate on local map and print entries
	UlongToDrvdPropPlanMapIter hmulpdpiter(m_phmulpdpCTEs);
	while (hmulpdpiter.Advance())
	{
		ULONG id = *(hmulpdpiter.Key());
		CDrvdPropPlan *pdpplan =
			const_cast<CDrvdPropPlan *>(hmulpdpiter.Value());

		os << id << "-->" << *pdpplan << std::endl;
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::PdpplanCTEProducer
//
//	@doc:
//		Return the plan properties of cte producer with given id
//
//---------------------------------------------------------------------------
CDrvdPropPlan *
CDrvdPropCtxtPlan::PdpplanCTEProducer(ULONG ulCTEId) const
{
	GPOS_ASSERT(NULL != m_phmulpdpCTEs);

	return m_phmulpdpCTEs->Find(&ulCTEId);
}


//---------------------------------------------------------------------------
//	@function:
//		CDrvdPropCtxtPlan::CopyCTEProducerProps
//
//	@doc:
//		Copy CTE producer plan properties to local map
//
//---------------------------------------------------------------------------
void
CDrvdPropCtxtPlan::CopyCTEProducerProps(CDrvdPropPlan *pdpplan, ULONG ulCTEId)
{
	GPOS_ASSERT(NULL != pdpplan);

	pdpplan->AddRef();
#ifdef GPOS_DEBUG
	BOOL fInserted =
#endif	// GPOS_DEBUG
		m_phmulpdpCTEs->Insert(GPOS_NEW(m_mp) ULONG(ulCTEId), pdpplan);
	GPOS_ASSERT(fInserted);
}