//---------------------------------------------------------------------------
//	@filename:
//		CMDProviderGeneric.cpp
//
//	@doc:
//		Implementation of a generic MD provider.
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/CMDProviderGeneric.h"
#include "duckdb/optimizer/cascade/error/CAutoTrace.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/memory/CMemoryPool.h"
#include "duckdb/optimizer/cascade/exception.h"
#include "duckdb/optimizer/cascade/base/CUtils.h"
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderGeneric::CMDProviderGeneric
//
//	@doc:
//		Constructs a file-based metadata provider
//
//---------------------------------------------------------------------------
CMDProviderGeneric::CMDProviderGeneric(CMemoryPool *mp)
{
	// TODO:  - Jan 25, 2012; those should not be tied to a particular system
	m_mdid_int2 = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT2);
	m_mdid_int4 = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT4);
	m_mdid_int8 = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8);
	m_mdid_bool = GPOS_NEW(mp) CMDIdGPDB(GPDB_BOOL);
	m_mdid_oid = GPOS_NEW(mp) CMDIdGPDB(GPDB_OID);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderGeneric::~CMDProviderGeneric
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CMDProviderGeneric::~CMDProviderGeneric()
{
	m_mdid_int2->Release();
	m_mdid_int4->Release();
	m_mdid_int8->Release();
	m_mdid_bool->Release();
	m_mdid_oid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderGeneric::MDId
//
//	@doc:
//		return the mdid of a requested type
//
//---------------------------------------------------------------------------
IMDId *
CMDProviderGeneric::MDId(IMDType::ETypeInfo type_info) const
{
	GPOS_ASSERT(IMDType::EtiGeneric > type_info);

	switch (type_info)
	{
		case IMDType::EtiInt2:
			return m_mdid_int2;

		case IMDType::EtiInt4:
			return m_mdid_int4;

		case IMDType::EtiInt8:
			return m_mdid_int8;

		case IMDType::EtiBool:
			return m_mdid_bool;

		case IMDType::EtiOid:
			return m_mdid_oid;

		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderGeneric::SysidDefault
//
//	@doc:
//		Get the default system id of the MD provider
//
//---------------------------------------------------------------------------
CSystemId CMDProviderGeneric::SysidDefault() const
{
	return CSystemId(IMDId::EmdidGPDB, GPMD_GPDB_SYSID);
}