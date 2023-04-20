//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (c) 2020 VMware and affiliates, Inc.
//
// CPatternNode.h
//
// A node that matches multiple operators, to be used in patterns of xforms.
// The exact matching algorithm depends on an enum that is passed in the
// constructor.
//---------------------------------------------------------------------------
#ifndef GPOPT_CPatternNode_H
#define GPOPT_CPatternNode_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/operators/CPattern.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
// CPatternNode
//
// An operator that can match multiple other operator types
//---------------------------------------------------------------------------
class CPatternNode : public CPattern
{
public:
	enum EMatchType
	{
		EmtMatchInnerOrLeftOuterJoin,
		EmtSentinel
	};

	enum EMatchType m_match;

public:
	CPatternNode(COperator &) = delete;

	// ctor
	explicit CPatternNode(CMemoryPool *mp, enum EMatchType matchType)
		: CPattern(mp), m_match(matchType)
	{
	}

	// dtor
	~CPatternNode() override = default;

	// match function
	BOOL
	Matches(COperator *pop) const override
	{
		return Eopid() == pop->Eopid() &&
			   m_match == static_cast<CPatternNode *>(pop)->m_match;
	}

	// check if operator is a pattern leaf
	BOOL
	FLeaf() const override
	{
		return false;
	}

	// conversion function
	static CPatternNode *
	PopConvert(COperator *pop)
	{
		GPOS_ASSERT(NULL != pop);
		GPOS_ASSERT(COperator::EopPatternNode == pop->Eopid());

		return static_cast<CPatternNode *>(pop);
	}

	// ident accessors
	EOperatorId
	Eopid() const override
	{
		return EopPatternNode;
	}

	// return a string for operator name
	const CHAR *
	SzId() const override
	{
		return "CPatternNode";
	}

	BOOL
	MatchesOperator(enum COperator::EOperatorId opid)
	{
		switch (m_match)
		{
			case EmtMatchInnerOrLeftOuterJoin:
				return COperator::EopLogicalInnerJoin == opid ||
					   COperator::EopLogicalLeftOuterJoin == opid;

			default:
				return false;
		}
	}

};	// class CPatternNode
}  // namespace gpopt


#endif	// !GPOPT_CPatternNode_H

// EOF
