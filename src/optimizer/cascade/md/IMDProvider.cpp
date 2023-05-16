//---------------------------------------------------------------------------
//	@filename:
//		IMDProvider.cpp
//
//	@doc:
//		Abstract class for retrieving metadata from an external location
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/IMDProvider.h"
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"

using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDProvider::GetGPDBTypeMdid
//
//	@doc:
//		Return the mdid for the requested type
//
//---------------------------------------------------------------------------
IMDId *
IMDProvider::GetGPDBTypeMdid(CMemoryPool *mp,
							 CSystemId
#ifdef GPOS_DEBUG
								 sysid
#endif	// GPOS_DEBUG
							 ,
							 IMDType::ETypeInfo type_info)
{
	GPOS_ASSERT(IMDId::EmdidGPDB == sysid.MdidType());
	GPOS_ASSERT(IMDType::EtiGeneric > type_info);

	switch (type_info)
	{
		case IMDType::EtiInt2:
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_INT2);

		case IMDType::EtiInt4:
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4);

		case IMDType::EtiInt8:
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8);

		case IMDType::EtiBool:
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_BOOL);

		case IMDType::EtiOid:
			return GPOS_NEW(mp) CMDIdGPDB(GPDB_OID);

		default:
			return NULL;
	}
}