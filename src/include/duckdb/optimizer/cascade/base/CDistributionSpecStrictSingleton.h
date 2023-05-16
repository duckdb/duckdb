//---------------------------------------------------------------------------
//	@filename:
//		CDistributionSpecStrictSingleton.h
//
//	@doc:
//		Description of a strict singleton distribution. Unlike the simple singleton distribution,
//		that satisfies hash distribution requests, the strict singleton distribution is only
//		compatible with other singleton distributions
//		Can be used as a derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_CDistributionSpecStrictSingleton_H
#define GPOPT_CDistributionSpecStrictSingleton_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/utils.h"
#include "duckdb/optimizer/cascade/base/CDistributionSpecSingleton.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecStrictSingleton
//
//	@doc:
//		Class for representing singleton distribution specification.
//
//---------------------------------------------------------------------------
class CDistributionSpecStrictSingleton : public CDistributionSpecSingleton
{
private:
	// copy ctor
	CDistributionSpecStrictSingleton(const CDistributionSpecStrictSingleton &);

public:
	// ctor
	explicit CDistributionSpecStrictSingleton(ESegmentType esegtype);

	// distribution type accessor
	virtual EDistributionType
	Edt() const
	{
		return CDistributionSpec::EdtStrictSingleton;
	}

	// return true if distribution spec can be required
	virtual BOOL
	FRequirable() const
	{
		return false;
	}

	// does this distribution satisfy the given one
	virtual BOOL FSatisfies(const CDistributionSpec *pds) const;

	// append enforcers to dynamic array for the given plan properties
	virtual void
	AppendEnforcers(CMemoryPool *,		  // mp
					CExpressionHandle &,  // exprhdl
					CReqdPropPlan *,	  // prpp
					CExpressionArray *,	  // pdrgpexpr
					CExpression *		  // pexpr
	)
	{
		GPOS_ASSERT(!"attempt to enforce strict SINGLETON distribution");
	}

	// print
	virtual IOstream &OsPrint(IOstream &os) const;

	// conversion function
	static CDistributionSpecStrictSingleton *
	PdssConvert(CDistributionSpec *pds)
	{
		GPOS_ASSERT(NULL != pds);
		GPOS_ASSERT(EdtStrictSingleton == pds->Edt());

		return dynamic_cast<CDistributionSpecStrictSingleton *>(pds);
	}

};	// class CDistributionSpecStrictSingleton

}  // namespace gpopt

#endif