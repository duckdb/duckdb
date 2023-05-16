//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2020 VMware, Inc.
//
//	@filename:
//		CStatsPredArrayCmp.cpp
//
//	@doc:
//		Implementation of statistics for ArrayCmp filter
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/statistics/CStatsPredArrayCmp.h"
#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/base/CColRefTable.h"
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"

using namespace gpnaucrates;
using namespace gpopt;
using namespace gpmd;

// Ctor
CStatsPredArrayCmp::CStatsPredArrayCmp(ULONG colid, CStatsPred::EStatsCmpType stats_cmp_type, CPointArray *points)
	: CStatsPred(colid), m_stats_cmp_type(stats_cmp_type), m_points(points)
{
	GPOS_ASSERT(CStatsPred::EstatscmptEq == m_stats_cmp_type);
}