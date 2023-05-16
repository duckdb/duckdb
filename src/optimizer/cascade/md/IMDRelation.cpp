//---------------------------------------------------------------------------
//	@filename:
//		IMDRelation.cpp
//
//	@doc:
//		Implementation
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/IMDRelation.h"
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
const CWStringConst *
IMDRelation::GetDistrPolicyStr(Ereldistrpolicy rel_distr_policy)
{
	switch (rel_distr_policy)
	{
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		IMDRelation::GetStorageTypeStr
//
//	@doc:
//		Return name of storage type
//
//---------------------------------------------------------------------------
const CWStringConst *
IMDRelation::GetStorageTypeStr(IMDRelation::Erelstoragetype rel_storage_type)
{
	switch (rel_storage_type)
	{
		default:
			return NULL;
	}
}

// check if index is partial given its mdid
BOOL IMDRelation::IsPartialIndex(IMDId *	 // mdid
) const
{
	return false;
}