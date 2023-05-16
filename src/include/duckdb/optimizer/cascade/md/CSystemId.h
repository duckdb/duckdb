//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSystemId.h
//
//	@doc:
//		Class for representing the system id of a metadata provider
//---------------------------------------------------------------------------
#ifndef GPMD_CSystemId_H
#define GPMD_CSystemId_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/md/IMDId.h"

#define GPDXL_SYSID_LENGTH 10

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CSystemId
//
//	@doc:
//		Class for representing the system id of a metadata provider
//
//---------------------------------------------------------------------------
class CSystemId
{
private:
	// system id type
	IMDId::EMDIdType m_mdid_type;

	// system id
	WCHAR m_sysid_char[GPDXL_SYSID_LENGTH + 1];

public:
	// ctor
	CSystemId(IMDId::EMDIdType mdid_type, const WCHAR *sysid_char,
			  ULONG length = GPDXL_SYSID_LENGTH);

	// copy ctor
	CSystemId(const CSystemId &);

	// type of system id
	IMDId::EMDIdType
	MdidType() const
	{
		return m_mdid_type;
	}

	// system id string
	const WCHAR *
	GetBuffer() const
	{
		return m_sysid_char;
	}

	// equality
	BOOL Equals(const CSystemId &sysid) const;

	// hash function
	ULONG HashValue() const;
};

// dynamic arrays over md system id elements
typedef CDynamicPtrArray<CSystemId, CleanupDelete> CSystemIdArray;
}  // namespace gpmd

#endif