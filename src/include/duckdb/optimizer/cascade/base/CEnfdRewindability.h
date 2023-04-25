//---------------------------------------------------------------------------
//	@filename:
//		CEnfdRewindability.h
//
//	@doc:
//		Enforceable rewindability property
//---------------------------------------------------------------------------
#ifndef GPOPT_CEnfdRewindability_H
#define GPOPT_CEnfdRewindability_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/CEnfdProp.h"
#include "duckdb/optimizer/cascade/base/CRewindabilitySpec.h"

namespace gpopt
{
using namespace gpos;

// prototypes
class CPhysical;


//---------------------------------------------------------------------------
//	@class:
//		CEnfdRewindability
//
//	@doc:
//		Enforceable rewindability property;
//
//---------------------------------------------------------------------------
class CEnfdRewindability : public CEnfdProp
{
public:
	// type of rewindability matching function
	enum ERewindabilityMatching
	{
		ErmSatisfy = 0,

		ErmSentinel
	};

private:
	// required rewindability
	CRewindabilitySpec *m_prs;

	// rewindability matching type
	ERewindabilityMatching m_erm;

	// private copy ctor
	CEnfdRewindability(const CEnfdRewindability &);

	// names of rewindability matching types
	static const CHAR *m_szRewindabilityMatching[ErmSentinel];

public:
	// ctor
	CEnfdRewindability(CRewindabilitySpec *prs, ERewindabilityMatching erm);

	// dtor
	virtual ~CEnfdRewindability();

	// hash function
	virtual ULONG HashValue() const;

	// check if the given rewindability specification is compatible with the
	// rewindability specification of this object for the specified matching type
	BOOL FCompatible(CRewindabilitySpec *pos) const;

	// required rewindability accessor
	CRewindabilitySpec *
	PrsRequired() const
	{
		return m_prs;
	}

	// get rewindability enforcing type for the given operator
	EPropEnforcingType Epet(CExpressionHandle &exprhdl, CPhysical *popPhysical,
							BOOL fRewindabilityReqd) const;

	// property spec accessor
	virtual CPropSpec *
	Pps() const
	{
		return m_prs;
	}

	// matching type accessor
	ERewindabilityMatching
	Erm() const
	{
		return m_erm;
	}

	// matching function
	BOOL
	Matches(CEnfdRewindability *per)
	{
		GPOS_ASSERT(NULL != per);

		return m_erm == per->Erm() && m_prs->Matches(per->PrsRequired());
	}


	// print function
	virtual IOstream &OsPrint(IOstream &os) const;

};	// class CEnfdRewindability

}  // namespace gpopt

#endif
