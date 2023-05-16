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

namespace gpopt
{
using namespace gpos;

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
private:
	// private copy ctor
	CPatternLeaf(const CPatternLeaf &);

public:
	// ctor
	explicit CPatternLeaf(CMemoryPool *mp) : CPattern(mp)
	{
	}

	// dtor
	virtual ~CPatternLeaf()
	{
	}

	// check if operator is a pattern leaf
	virtual BOOL
	FLeaf() const
	{
		return true;
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPatternLeaf;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CPatternLeaf";
	}

};	// class CPatternLeaf

}  // namespace gpopt

#endif