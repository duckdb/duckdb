//---------------------------------------------------------------------------
//	@filename:
//		CEnfdDistribution.cpp
//
//	@doc:
//		Implementation of enforceable distribution property
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CEnfdDistribution.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpec.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecHashed.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecSingleton.h"
#include "duckdb/optimizer/cascade/base/CDrvdPropPlan.h"
#include "duckdb/optimizer/cascade/base/CPartIndexMap.h"
#include "duckdb/optimizer/cascade/base/CReqdPropPlan.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionBroadcast.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionGather.h"
#include "duckdb/optimizer/cascade/operators/CPhysicalMotionHashDistribute.h"

using namespace gpopt;

// initialization of static variables
const CHAR *CEnfdDistribution::m_szDistributionMatching[EdmSentinel] = {
	"exact", "satisfy", "subset"};


//---------------------------------------------------------------------------
//	@function:
//		CEnfdDistribution::CEnfdDistribution
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CEnfdDistribution::CEnfdDistribution(CDistributionSpec *pds,
									 EDistributionMatching edm)
	: m_pds(pds), m_edm(edm)
{
	GPOS_ASSERT(NULL != pds);
	GPOS_ASSERT(EdmSentinel > edm);
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdDistribution::~CEnfdDistribution
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CEnfdDistribution::~CEnfdDistribution()
{
	CRefCount::SafeRelease(m_pds);
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdDistribution::FCompatible
//
//	@doc:
//		Check if the given distribution specification is compatible with the
//		distribution specification of this object for the specified matching type
//
//---------------------------------------------------------------------------
BOOL
CEnfdDistribution::FCompatible(CDistributionSpec *pds) const
{
	GPOS_ASSERT(NULL != pds);

	switch (m_edm)
	{
		case EdmExact:
			return pds->Matches(m_pds);

		case EdmSatisfy:
			return pds->FSatisfies(m_pds);

		case EdmSubset:
			if (CDistributionSpec::EdtHashed == m_pds->Edt() &&
				CDistributionSpec::EdtHashed == pds->Edt())
			{
				return CDistributionSpecHashed::PdsConvert(pds)->FMatchSubset(
					CDistributionSpecHashed::PdsConvert(m_pds));
			}

		case (EdmSentinel):
			GPOS_ASSERT("invalid matching type");
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CEnfdDistribution::HashValue
//
//	@doc:
// 		Hash function
//
//---------------------------------------------------------------------------
ULONG
CEnfdDistribution::HashValue() const
{
	return gpos::CombineHashes(m_edm + 1, m_pds->HashValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CEnfdDistribution::Epet
//
//	@doc:
// 		Get distribution enforcing type for the given operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CEnfdDistribution::Epet(CExpressionHandle &exprhdl, CPhysical *popPhysical,
						CPartitionPropagationSpec *pppsReqd,
						BOOL fDistribReqd) const
{
	if (fDistribReqd)
	{
		CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();

		if (CDistributionSpec::EdtReplicated == pds->Edt() &&
			CDistributionSpec::EdtHashed == PdsRequired()->Edt() &&
			EdmSatisfy == m_edm)
		{
			// child delivers a replicated distribution, no need to enforce hashed distribution
			// if only satisfiability is needed
			return EpetUnnecessary;
		}

		// if operator is a propagator/consumer of any partition index id, prohibit
		// enforcing any distribution not compatible with what operator delivers
		// if the derived partition consumers are a subset of the ones in the given
		// required partition propagation spec, those will be enforced in the same group
		CPartIndexMap *ppimDrvd = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Ppim();
		GPOS_ASSERT(NULL != ppimDrvd);
		if (ppimDrvd->FContainsUnresolved() && !this->FCompatible(pds) &&
			!ppimDrvd->FSubset(pppsReqd->Ppim()))
		{
			return CEnfdProp::EpetProhibited;
		}

		// N.B.: subtlety ahead:
		// We used to do the following check in CPhysicalMotion::FValidContext
		// which was correct but more costly. We are able to move the outer
		// reference check here because:
		// 1. The fact that execution has reached here means that we are about
		// to add ("enforce") a motion into this group.
		// 2. The number of outer references is a logical ("relational")
		// property, which means *every* group expression in the current group
		// has the same number of outer references.
		// 3. The following logic ensures that *none* of the group expressions
		// will add a motion into this group.
		EPropEnforcingType epet = popPhysical->EpetDistribution(exprhdl, this);
		if (FEnforce(epet) && exprhdl.HasOuterRefs())
		{
			// disallow plans with outer references below motion operator
			return EpetProhibited;
		}
		else
		{
			return epet;
		}
	}

	return EpetUnnecessary;
}

//---------------------------------------------------------------------------
//	@function:
//		CEnfdDistribution::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream& CEnfdDistribution::OsPrint(IOstream &os) const
{
	os = m_pds->OsPrint(os);
	return os << " match: " << m_szDistributionMatching[m_edm];
}