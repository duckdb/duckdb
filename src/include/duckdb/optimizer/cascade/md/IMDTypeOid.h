//---------------------------------------------------------------------------
//	@filename:
//		IMDTypeOid.h
//
//	@doc:
//		Interface for OID types in the metadata cache
//---------------------------------------------------------------------------
#ifndef GPMD_IMDTypeOid_H
#define GPMD_IMDTypeOid_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
class IDatumOid;
}

namespace gpmd
{
using namespace gpos;
using namespace gpnaucrates;


//---------------------------------------------------------------------------
//	@class:
//		IMDTypeOid
//
//	@doc:
//		Interface for OID types in the metadata cache
//
//---------------------------------------------------------------------------
class IMDTypeOid : public IMDType
{
public:
	// type id
	static ETypeInfo
	GetTypeInfo()
	{
		return EtiOid;
	}

	virtual ETypeInfo
	GetDatumType() const
	{
		return IMDTypeOid::GetTypeInfo();
	}

	// factory function for OID datums
	virtual IDatumOid *CreateOidDatum(CMemoryPool *mp, OID oid_value,
									  BOOL is_null) const = 0;
};
}  // namespace gpmd

#endif