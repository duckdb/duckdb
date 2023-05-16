//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
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
private:
	// private copy ctor
	CPatternTree(const CPatternTree &);

public:
	// ctor
	explicit CPatternTree(CMemoryPool *mp) : CPattern(mp)
	{
	}

	// dtor
	virtual ~CPatternTree()
	{
	}

	// check if operator is a pattern leaf
	virtual BOOL
	FLeaf() const
	{
		return false;
	}

	// ident accessors
	virtual EOperatorId
	Eopid() const
	{
		return EopPatternTree;
	}

	// return a string for operator name
	virtual const CHAR *
	SzId() const
	{
		return "CPatternTree";
	}

};	// class CPatternTree

}  // namespace gpopt

#endif