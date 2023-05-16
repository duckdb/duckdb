//---------------------------------------------------------------------------
//	@filename:
//		CDistributionSpecNonSingleton.h
//
//	@doc:
//		Description of a general distribution which imposes no singleton
//		distribution requirements;
//		Can be used only as a required property;
//---------------------------------------------------------------------------
#ifndef GPOPT_CDistributionSpecNonSingleton_H
#define GPOPT_CDistributionSpecNonSingleton_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpec.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecNonSingleton
//
//	@doc:
//		Class for representing general distribution specification which
//		imposes no requirements.
//
//---------------------------------------------------------------------------
class CDistributionSpecNonSingleton : public CDistributionSpec
{
private:
	// should Replicated distribution satisfy current distribution
	BOOL m_fAllowReplicated;

	// private copy ctor
	CDistributionSpecNonSingleton(const CDistributionSpecNonSingleton &);

public:
	//ctor
	CDistributionSpecNonSingleton();

	//ctor
	explicit CDistributionSpecNonSingleton(BOOL fAllowReplicated);

	// should Replicated distribution satisfy current distribution
	BOOL
	FAllowReplicated() const
	{
		return m_fAllowReplicated;
	}

	// accessor
	virtual EDistributionType
	Edt() const
	{
		return CDistributionSpec::EdtNonSingleton;
	}

	// does current distribution satisfy the given one
	virtual BOOL FSatisfies(const CDistributionSpec *pds) const;

	// append enforcers to dynamic array for the given plan properties
	virtual void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
								 CReqdPropPlan *prpp,
								 CExpressionArray *pdrgpexpr,
								 CExpression *pexpr);

	// return distribution partitioning type
	virtual EDistributionPartitioningType
	Edpt() const
	{
		// a non-singleton distribution could be replicated to all segments, or partitioned across segments
		return EdptUnknown;
	}

	// return true if distribution spec can be derived
	virtual BOOL
	FDerivable() const
	{
		return false;
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	// conversion function
	static CDistributionSpecNonSingleton *
	PdsConvert(CDistributionSpec *pds)
	{
		GPOS_ASSERT(NULL != pds);
		GPOS_ASSERT(EdtNonSingleton == pds->Edt());

		return dynamic_cast<CDistributionSpecNonSingleton *>(pds);
	}

};	// class CDistributionSpecNonSingleton

}  // namespace gpopt

#endif