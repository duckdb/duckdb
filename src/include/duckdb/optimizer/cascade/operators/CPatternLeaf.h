//---------------------------------------------------------------------------
//	@filename:
//		CPatternLeaf.h
//
//	@doc:
//		Pattern that matches a single leaf
//---------------------------------------------------------------------------
#ifndef GPOPT_CPatternLeaf_H
#define GPOPT_CPatternLeaf_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPattern.h"

using namespace gpos;
using namespace duckdb;

namespace gpopt
{
//---------------------------------------------------------------------------
//	@class:
//		CPatternLeaf
//
//	@doc:
//		Pattern that matches a single leaf
//
//---------------------------------------------------------------------------
class CPatternLeaf : public CPattern
{
public:
	// ctor
	explicit CPatternLeaf()
		: CPattern()
	{
	}

	// private copy ctor
	CPatternLeaf(const CPatternLeaf &) = delete;

	// dtor
	virtual ~CPatternLeaf()
	{
	}

	// check if operator is a pattern leaf
	virtual bool FLeaf() const override
	{
		return true;
	}

	// return a string for operator name
	virtual const CHAR* SzId() const
	{
		return "CPatternLeaf";
	}

	duckdb::unique_ptr<Operator> CopywithNewGroupExpression(CGroupExpression* pgexpr) override
	{
		return make_uniq<CPatternLeaf>();
	}
};	// class CPatternLeaf
}  // namespace gpopt
#endif