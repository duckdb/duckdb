//---------------------------------------------------------------------------
//	@filename:
//		IMDTypeInt4.h
//
//	@doc:
//		Interface for INT4 types in the metadata cache
//---------------------------------------------------------------------------

#ifndef GPMD_IMDTypeInt4_H
#define GPMD_IMDTypeInt4_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/md/IMDType.h"

// fwd decl
namespace gpnaucrates
{
class IDatumInt4;
}

namespace gpmd
{
using namespace gpos;
using namespace gpnaucrates;


//---------------------------------------------------------------------------
//	@class:
//		IMDTypeInt4
//
//	@doc:
//		Interface for INT4 types in the metadata cache
//
//---------------------------------------------------------------------------
class IMDTypeInt4 : public IMDType
{
public:
	// type id
	static ETypeInfo
	GetTypeInfo()
	{
		return EtiInt4;
	}

	virtual ETypeInfo
	GetDatumType() const
	{
		return IMDTypeInt4::GetTypeInfo();
	}

	// factory function for INT4 datums
	virtual IDatumInt4 *CreateInt4Datum(CMemoryPool *mp, INT value,
										BOOL is_null) const = 0;
};

}  // namespace gpmd

#endif
