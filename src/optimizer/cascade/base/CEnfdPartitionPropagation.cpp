//---------------------------------------------------------------------------
//	@filename:
//		CEnfdPartitionPropagation.cpp
//
//	@doc:
//		Implementation of enforced partition propagation property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CEnfdPartitionPropagation.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
#include "duckdb/optimizer/cascade/operators/CPhysical.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::CEnfdPartitionPropagation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEnfdPartitionPropagation::CEnfdPartitionPropagation(CPartitionPropagationSpec *ppps, EPartitionPropagationMatching eppm, CPartFilterMap *ppfm)
	: m_ppps(ppps), m_eppm(eppm), m_ppfmDerived(ppfm)
{
	GPOS_ASSERT(NULL != ppps);
	GPOS_ASSERT(EppmSentinel > eppm);
	GPOS_ASSERT(NULL != ppfm);
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::~CEnfdPartitionPropagation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEnfdPartitionPropagation::~CEnfdPartitionPropagation()
{
	m_ppps->Release();
	m_ppfmDerived->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::HashValue
//
//	@doc:
// 		Hash function
//
//---------------------------------------------------------------------------
ULONG
CEnfdPartitionPropagation::HashValue() const
{
	return m_ppps->HashValue();
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::Epet
//
//	@doc:
// 		Get partition propagation enforcing type for the given operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CEnfdPartitionPropagation::Epet(CExpressionHandle &exprhdl,
								CPhysical *popPhysical,
								BOOL fPropagationReqd) const
{
	if (fPropagationReqd)
	{
		return popPhysical->EpetPartitionPropagation(exprhdl, this);
	}

	return EpetUnnecessary;
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::FResolved
//
//	@doc:
// 		Is required partition propagation resolved by the given part index map
//
//---------------------------------------------------------------------------
BOOL
CEnfdPartitionPropagation::FResolved(CMemoryPool *mp, CPartIndexMap *ppim) const
{
	GPOS_ASSERT(NULL != ppim);

	CPartIndexMap *ppimReqd = m_ppps->Ppim();
	if (!ppimReqd->FContainsUnresolved())
	{
		return true;
	}

	ULongPtrArray *pdrgpulPartIndexIds = ppimReqd->PdrgpulScanIds(mp);
	const ULONG length = pdrgpulPartIndexIds->Size();

	BOOL fResolved = true;
	for (ULONG ul = 0; ul < length && fResolved; ul++)
	{
		ULONG part_idx_id = *((*pdrgpulPartIndexIds)[ul]);
		GPOS_ASSERT(CPartIndexMap::EpimConsumer == ppimReqd->Epim(part_idx_id));

		// check whether part index id has been resolved in the derived map
		fResolved = false;
		if (ppim->Contains(part_idx_id))
		{
			CPartIndexMap::EPartIndexManipulator epim = ppim->Epim(part_idx_id);
			ULONG ulExpectedPropagators =
				ppim->UlExpectedPropagators(part_idx_id);

			fResolved = CPartIndexMap::EpimResolver == epim ||
						CPartIndexMap::EpimPropagator == epim ||
						(CPartIndexMap::EpimConsumer == epim &&
						 0 < ulExpectedPropagators &&
						 ppimReqd->UlExpectedPropagators(part_idx_id) ==
							 ulExpectedPropagators);
		}
	}

	// cleanup
	pdrgpulPartIndexIds->Release();

	return fResolved;
}

//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::FInScope
//
//	@doc:
// 		Is required partition propagation in the scope defined by the given part index map
//
//---------------------------------------------------------------------------
BOOL
CEnfdPartitionPropagation::FInScope(CMemoryPool *mp, CPartIndexMap *ppim) const
{
	GPOS_ASSERT(NULL != ppim);

	CPartIndexMap *ppimReqd = m_ppps->Ppim();

	ULongPtrArray *pdrgpulPartIndexIds = ppimReqd->PdrgpulScanIds(mp);
	const ULONG length = pdrgpulPartIndexIds->Size();

	if (0 == length)
	{
		pdrgpulPartIndexIds->Release();
		return true;
	}

	BOOL fInScope = true;
	for (ULONG ul = 0; ul < length && fInScope; ul++)
	{
		ULONG part_idx_id = *((*pdrgpulPartIndexIds)[ul]);
		GPOS_ASSERT(CPartIndexMap::EpimConsumer == ppimReqd->Epim(part_idx_id));

		// check whether part index id exists in the derived part consumers
		fInScope = ppim->Contains(part_idx_id);
	}

	// cleanup
	pdrgpulPartIndexIds->Release();

	return fInScope;
}

//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CEnfdPartitionPropagation::OsPrint(IOstream &os) const
{
	return os << (*m_ppps) << " match: " << SzPropagationMatching(m_eppm)
			  << " ";
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdPartitionPropagation::SzPropagationMatching
//
//	@doc:
//		Propagation matching string
//
//---------------------------------------------------------------------------
const CHAR* CEnfdPartitionPropagation::SzPropagationMatching(EPartitionPropagationMatching eppm)
{
	GPOS_ASSERT(EppmSentinel > eppm);
	const CHAR *rgszPropagationMatching[EppmSentinel] = {"satisfy"};
	return rgszPropagationMatching[eppm];
}