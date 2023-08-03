//---------------------------------------------------------------------------
//	@filename:
//		CPatternMultiLeaf.h
//
//	@doc:
//		Pattern that matches a variable number of leaves
//---------------------------------------------------------------------------
#ifndef GPOPT_CPatternMultiLeaf_H
#define GPOPT_CPatternMultiLeaf_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPattern.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CPatternMultiLeaf
//
//	@doc:
//		Pattern that matches a variable number of expressions, eg inputs to
//		union operator
//
//---------------------------------------------------------------------------
class CPatternMultiLeaf : public CPattern
{
private:
	// private copy ctor
	CPatternMultiLeaf(const CPatternMultiLeaf &);

public:
	// ctor
	explicit CPatternMultiLeaf()
		: CPattern()
	{
	}

	// dtor
	virtual ~CPatternMultiLeaf()
	{
	}

	// check if operator is a pattern leaf
	virtual bool FLeaf() const
	{
		return true;
	}

	// return a string for operator name
	virtual const CHAR* SzId() const
	{
		return "CPatternMultiLeaf";
	}
};	// class CPatternMultiLeaf
}  // namespace gpopt
#endif