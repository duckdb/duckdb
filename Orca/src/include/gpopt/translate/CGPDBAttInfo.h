//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CGPDBAttInfo.h
//
//	@doc:
//		Class to uniquely identify a column in GPDB
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CGPDBAttInfo_H
#define GPDXL_CGPDBAttInfo_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/utils.h"

#include "naucrates/dxl/gpdb_types.h"

namespace gpdxl
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CGPDBAttInfo
//
//	@doc:
//		Class to uniquely identify a column in GPDB
//
//---------------------------------------------------------------------------
class CGPDBAttInfo : public CRefCount
{
private:
	// query level number
	ULONG m_query_level;

	// varno in the rtable
	ULONG m_varno;

	// attno
	INT m_attno;

public:
	CGPDBAttInfo(const CGPDBAttInfo &) = delete;

	// ctor
	CGPDBAttInfo(ULONG query_level, ULONG var_no, INT attrnum)
		: m_query_level(query_level), m_varno(var_no), m_attno(attrnum)
	{
	}

	// d'tor
	~CGPDBAttInfo() override = default;

	// accessor
	ULONG
	GetQueryLevel() const
	{
		return m_query_level;
	}

	// accessor
	ULONG
	GetVarNo() const
	{
		return m_varno;
	}

	// accessor
	INT
	GetAttNo() const
	{
		return m_attno;
	}

	// equality check
	BOOL
	Equals(const CGPDBAttInfo &gpdb_att_info) const
	{
		return m_query_level == gpdb_att_info.m_query_level &&
			   m_varno == gpdb_att_info.m_varno &&
			   m_attno == gpdb_att_info.m_attno;
	}

	// hash value
	ULONG
	HashValue() const
	{
		return gpos::CombineHashes(
			gpos::HashValue(&m_query_level),
			gpos::CombineHashes(gpos::HashValue(&m_varno),
								gpos::HashValue(&m_attno)));
	}
};

// hash function
inline ULONG
HashGPDBAttInfo(const CGPDBAttInfo *gpdb_att_info)
{
	GPOS_ASSERT(NULL != gpdb_att_info);
	return gpdb_att_info->HashValue();
}

// equality function
inline BOOL
EqualGPDBAttInfo(const CGPDBAttInfo *gpdb_att_info_a,
				 const CGPDBAttInfo *gpdb_att_info_b)
{
	GPOS_ASSERT(NULL != gpdb_att_info_a && NULL != gpdb_att_info_b);
	return gpdb_att_info_a->Equals(*gpdb_att_info_b);
}

}  // namespace gpdxl

#endif	// !GPDXL_CGPDBAttInfo_H

// EOF
