//---------------------------------------------------------------------------
//	@filename:
//		CMDProviderGeneric.h
//
//	@doc:
//		Provider of system-independent metadata objects.
//---------------------------------------------------------------------------
#ifndef GPMD_CMDProviderGeneric_H
#define GPMD_CMDProviderGeneric_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/IMDId.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

#define GPMD_DEFAULT_SYSID GPOS_WSZ_LIT("GPDB")

namespace gpmd
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CMDProviderGeneric
//
//	@doc:
//		Provider of system-independent metadata objects.
//
//---------------------------------------------------------------------------
class CMDProviderGeneric
{
private:
	// mdid of int2
	IMDId *m_mdid_int2;

	// mdid of int4
	IMDId *m_mdid_int4;

	// mdid of int8
	IMDId *m_mdid_int8;

	// mdid of bool
	IMDId *m_mdid_bool;

	// mdid of oid
	IMDId *m_mdid_oid;

	// private copy ctor
	CMDProviderGeneric(const CMDProviderGeneric &);

public:
	// ctor/dtor
	CMDProviderGeneric(CMemoryPool *mp);

	// dtor
	~CMDProviderGeneric();

	// return the mdid for the requested type
	IMDId *MDId(IMDType::ETypeInfo type_info) const;

	// default system id
	CSystemId SysidDefault() const;
};
}  // namespace gpmd

#endif