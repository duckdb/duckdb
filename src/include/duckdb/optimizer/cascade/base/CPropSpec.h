//---------------------------------------------------------------------------
//	@filename:
//		CPropSpec.h
//
//	@doc:
//		Abstraction for specification of properties;
//---------------------------------------------------------------------------
#ifndef GPOPT_CPropSpec_H
#define GPOPT_CPropSpec_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CRefCount.h"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/optimizer/cascade/operators/CExpression.h"

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

// prototypes
class CReqdPropPlan;

//---------------------------------------------------------------------------
//	@class:
//		CPropSpec
//
//	@doc:
//		Property specification
//
//---------------------------------------------------------------------------
class CPropSpec : public CRefCount
{
public:
	// property type
	enum EPropSpecType
	{
		EpstOrder,
		EpstDistribution,
		EpstRewindability,
		EpstPartPropagation,
		EpstSentinel
	};

private:
	// private copy ctor
	CPropSpec(const CPropSpec &);

protected:
	// ctor
	CPropSpec()
	{
	}

	// dtor
	~CPropSpec()
	{
	}

public:
	// append enforcers to dynamic array for the given plan properties
	virtual void AppendEnforcers(CMemoryPool* mp, CExpressionHandle &exprhdl, CReqdPropPlan* prpp, ExpressionArray* pdrgpexpr, unique_ptr<LogicalOperator> pexpr) = 0;

	// hash function
	virtual ULONG HashValue() const = 0;

	// extract columns used by the property
	virtual CColRefSet *PcrsUsed(CMemoryPool *mp) const = 0;

	// property type
	virtual EPropSpecType Epst() const = 0;
};	// class CPropSpec


// shorthand for printing
inline IOstream &
operator<<(IOstream &os, const CPropSpec &ospec)
{
	return ospec.OsPrint(os);
}

}  // namespace gpopt

#endif
