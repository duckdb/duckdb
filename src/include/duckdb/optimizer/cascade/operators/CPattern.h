//---------------------------------------------------------------------------
//	@filename:
//		CPattern.h
//
//	@doc:
//		Base class for all pattern operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CPattern_H
#define GPOPT_CPattern_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/operators/COperator.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CPattern
//
//	@doc:
//		base class for all pattern operators
//
//---------------------------------------------------------------------------
class CPattern : public COperator
{
private:
	// private copy ctor
	CPattern(const CPattern &);

public:
	// ctor
	explicit CPattern(CMemoryPool *mp) : COperator(mp)
	{
	}

	// dtor
	virtual ~CPattern()
	{
	}

	// type of operator
	virtual BOOL
	FPattern() const
	{
		GPOS_ASSERT(!FPhysical() && !FScalar() && !FLogical());
		return true;
	}

	// create derived properties container
	virtual CDrvdProp *PdpCreate(CMemoryPool *mp) const;

	// create required properties container
	virtual CReqdProp *PrpCreate(CMemoryPool *mp) const;

	// match function
	BOOL Matches(COperator *) const;

	// sensitivity to order of inputs
	BOOL FInputOrderSensitive() const;

	// check if operator is a pattern leaf
	virtual BOOL FLeaf() const = 0;

	// return a copy of the operator with remapped columns
	virtual COperator *PopCopyWithRemappedColumns(
		CMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

	// conversion function
	static CPattern *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(pop->FPattern());

		return reinterpret_cast<CPattern *>(pop);
	}
};	// class CPattern

}  // namespace gpopt

#endif