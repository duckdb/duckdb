//---------------------------------------------------------------------------
//	@filename:
//		CEnfdProp.h
//
//	@doc:
//		Base class for all enforceable properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CEnfdProp_H
#define GPOPT_CEnfdProp_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"

#include "duckdb/optimizer/cascade/base/CPropSpec.h"
#include "duckdb/optimizer/cascade/operators/COperator.h"


namespace gpopt
{
using namespace gpos;

// prototypes
class CPhysical;
class CReqdPropPlan;

//---------------------------------------------------------------------------
//	@class:
//		CEnfdProp
//
//	@doc:
//		Abstract base class for all enforceable properties.
//
//---------------------------------------------------------------------------
class CEnfdProp : public CRefCount
{
public:
	// Definition of property enforcing type for a given operator.
	//
	// Each enforced property is queried in CEngine::FCheckEnfdProps() to
	// determine if enforcers are required, optional, unnecessary or
	// prohibited over an operator given an optimization context of
	// properties required of it.
	//
	// - Required: operator cannot deliver the required properties on its
	// own, e.g., requiring a sort order from a table scan
	//
	// - Optional: operator can request the required properties from its children
	// and preserve them, e.g., requiring a sort order from a filter
	//
	// - Prohibited: operator prohibits enforcing the required properties on its
	// output, e.g., requiring a sort order on column A from a sort operator that
	// provides sorting on column B
	//
	// - Unnecessary: operator already establishes the required properties on its
	// own, e.g., requiring a sort order on column A from a sort operator that
	// provides sorting on column A. If the required property spec is empty, any
	// operator satisfies it so its type falls into this category.
	//
	// NB: 'Prohibited' prevents ANY enforcer to be added for the given
	// operator & optimization context, even if one is required by some other
	// enforced property.

	enum EPropEnforcingType
	{
		EpetRequired,
		EpetOptional,
		EpetProhibited,
		EpetUnnecessary,

		EpetSentinel
	};

private:
	// private copy ctor
	CEnfdProp(const CEnfdProp &);

public:
	// ctor
	CEnfdProp()
	{
	}

	// dtor
	virtual ~CEnfdProp()
	{
	}

	// append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(CMemoryPool* mp, CReqdPropPlan* prpp, CExpressionArray* pdrgpexpr, LogicalOperator* pexprChild, CEnfdProp::EPropEnforcingType epet, CExpressionHandle &exprhdl)
	{
		if (FEnforce(epet))
		{
			Pps()->AppendEnforcers(mp, exprhdl, prpp, pdrgpexpr, pexprChild);
		}
	}

	// property spec accessor
	virtual CPropSpec *Pps() const = 0;

	// hash function
	virtual ULONG HashValue() const = 0;

	// check if operator requires an enforcer under given enforceable property
	// based on the derived enforcing type
	static BOOL
	FEnforce(EPropEnforcingType epet)
	{
		return CEnfdProp::EpetOptional == epet ||
			   CEnfdProp::EpetRequired == epet;
	}

	// check if operator requires optimization under given enforceable property
	// based on the derived enforcing type
	static BOOL
	FOptimize(EPropEnforcingType epet)
	{
		return CEnfdProp::EpetOptional == epet ||
			   CEnfdProp::EpetUnnecessary == epet;
	}

};	// class CEnfdProp


// shorthand for printing
IOstream &operator<<(IOstream &os, CEnfdProp &efdprop);

}  // namespace gpopt

#endif
