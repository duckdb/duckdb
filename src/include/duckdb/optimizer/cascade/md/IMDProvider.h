//---------------------------------------------------------------------------
//	@filename:
//		IMDProvider.h
//
//	@doc:
//		Abstract class for retrieving metadata from an external location.
//---------------------------------------------------------------------------
#ifndef GPMD_IMDProvider_H
#define GPMD_IMDProvider_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CHashMap.h"
#include "duckdb/optimizer/cascade/string/CWStringBase.h"
#include "duckdb/optimizer/cascade/string/CWStringConst.h"
#include "duckdb/optimizer/cascade/md/IMDFunction.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		IMDProvider
//
//	@doc:
//		Abstract class for retrieving metadata from an external location.
//
//---------------------------------------------------------------------------
class IMDProvider : public CRefCount
{
protected:
	// return the mdid for the requested type
	static IMDId *GetGPDBTypeMdid(CMemoryPool *mp, CSystemId sysid,
								  IMDType::ETypeInfo type_info);

public:
	virtual ~IMDProvider()
	{
	}

	// returns the DXL string of the requested metadata object
	virtual CWStringBase *GetMDObjDXLStr(CMemoryPool *mp,
										 CMDAccessor *md_accessor,
										 IMDId *mdid) const = 0;

	// return the mdid for the specified system id and type
	virtual IMDId *MDId(CMemoryPool *mp, CSystemId sysid,
						IMDType::ETypeInfo type_info) const = 0;
};

// arrays of MD providers
typedef CDynamicPtrArray<IMDProvider, CleanupRelease> CMDProviderArray;

}  // namespace gpmd

#endif