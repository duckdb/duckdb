//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		IMDIndex.cpp
//
//	@doc:
//		Implementation of MD index
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/IMDIndex.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"

using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDRelation::GetDistrPolicyStr
//
//	@doc:
//		Return relation distribution policy as a string value
//
//---------------------------------------------------------------------------
const CWStringConst* IMDIndex::GetDXLStr(EmdindexType index_type)
{
	switch (index_type)
	{
		default:
			GPOS_ASSERT(!"Unrecognized index type");
			return NULL;
	}
}