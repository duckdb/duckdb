//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		CLogicalJoin.cpp
//
//	@doc:
//		Implementation of logical join class
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/operators/CLogicalJoin.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/base/CColRefSet.h"
#include "duckdb/optimizer/cascade/base/COptCtxt.h"
#include "duckdb/optimizer/cascade/operators/CExpression.h"
#include "duckdb/optimizer/cascade/operators/CExpressionHandle.h"
#include "duckdb/optimizer/cascade/operators/CPredicateUtils.h"
#include "duckdb/optimizer/cascade/statistics/CJoinStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::CLogicalJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalJoin::CLogicalJoin(CMemoryPool *mp)
    : CLogical(mp)
{
	GPOS_ASSERT(NULL != mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::Matches
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL CLogicalJoin::Matches(COperator *pop) const
{
	return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics* CLogicalJoin::PstatsDerive(CMemoryPool *mp, CExpressionHandle &exprhdl, IStatisticsArray *stats_ctxt) const
{
	return CJoinStatsProcessor::DeriveJoinStats(mp, exprhdl, stats_ctxt);
}