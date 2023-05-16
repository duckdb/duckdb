//---------------------------------------------------------------------------
//	@filename:
//		IMDTypeInt2.h
//
//	@doc:
//		Interface for INT2 types in the metadata cache
//---------------------------------------------------------------------------
#ifndef GPMD_IMDTypeInt2_H
#define GPMD_IMDTypeInt2_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
class IDatumInt2;
}

namespace gpmd
{
using namespace gpos;
using namespace gpnaucrates;


//---------------------------------------------------------------------------
//	@class:
//		IMDTypeInt2
//
//	@doc:
//		Interface for INT2 types in the metadata cache
//
//---------------------------------------------------------------------------
class IMDTypeInt2 : public IMDType
{
public:
	// type id
	static ETypeInfo
	GetTypeInfo()
	{
		return EtiInt2;
	}

	virtual ETypeInfo
	GetDatumType() const
	{
		return IMDTypeInt2::GetTypeInfo();
	}

	// factory function for INT2 datums
	virtual IDatumInt2 *CreateInt2Datum(CMemoryPool *mp, SINT value,
										BOOL is_null) const = 0;
};

}  // namespace gpmd

#endif