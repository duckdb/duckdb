//---------------------------------------------------------------------------
//	@filename:
//		IMDTypeBool.h
//
//	@doc:
//		Interface for BOOL types in the metadata cache
//---------------------------------------------------------------------------
#ifndef GPMD_IMDTypeBool_H
#define GPMD_IMDTypeBool_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
class IDatumBool;
}

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDTypeBool
//
//	@doc:
//		Interface for BOOL types in the metadata cache
//
//---------------------------------------------------------------------------
class IMDTypeBool : public IMDType
{
public:
	// type id
	static ETypeInfo
	GetTypeInfo()
	{
		return EtiBool;
	}

	virtual ETypeInfo
	GetDatumType() const
	{
		return IMDTypeBool::GetTypeInfo();
	}

	// factory function for BOOL datums
	virtual IDatumBool *CreateBoolDatum(CMemoryPool *mp, BOOL value, BOOL is_null) const = 0;
};

}  // namespace gpmd

#endif