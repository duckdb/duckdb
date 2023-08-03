//---------------------------------------------------------------------------
//	@filename:
//		CPatternTree.h
//
//	@doc:
//		Pattern that matches entire expression trees
//---------------------------------------------------------------------------
#ifndef GPOPT_CPatternTree_H
#define GPOPT_CPatternTree_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/CPattern.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CPatternTree
//
//	@doc:
//		Pattern that matches entire expression trees, e.g. scalar expressions
//
//---------------------------------------------------------------------------
class CPatternTree : public CPattern
{
public:
	// ctor
	explicit CPatternTree()
		: CPattern()
	{
	}

	CPatternTree(const CPatternTree &) = delete;

	// dtor
	virtual ~CPatternTree()
	{
	}

	// check if operator is a pattern leaf
	virtual bool FLeaf() const override
	{
		return false;
	}

	// return a string for operator name
	virtual const CHAR* SzId() const
	{
		return "CPatternTree";
	}

	duckdb::unique_ptr<Operator> CopywithNewGroupExpression(CGroupExpression* pgexpr) override
	{
		return make_uniq<CPatternTree>();
	}
};	// class CPatternTree
}  // namespace gpopt
#endif