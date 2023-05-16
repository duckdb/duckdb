//---------------------------------------------------------------------------
//	@filename:
//		CSystemId.cpp
//
//	@doc:
//		Implementation of system identifiers
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/CSystemId.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::CSystemId
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CSystemId::CSystemId(IMDId::EMDIdType mdid_type, const WCHAR *sysid_char, ULONG length)
	: m_mdid_type(mdid_type)
{
	GPOS_ASSERT(GPDXL_SYSID_LENGTH >= length);
	if (length > 0)
	{
		clib::WcStrNCpy(m_sysid_char, sysid_char, length);
	}
	// ensure string is terminated
	m_sysid_char[length] = WCHAR_EOS;
}

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::CSystemId
//
//	@doc:
//		Copy constructor
//
//---------------------------------------------------------------------------
CSystemId::CSystemId(const CSystemId &sysid) : m_mdid_type(sysid.MdidType())
{
	clib::WcStrNCpy(m_sysid_char, sysid.GetBuffer(), GPDXL_SYSID_LENGTH);
}

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL CSystemId::Equals(const CSystemId &sysid) const
{
	ULONG length = GPOS_WSZ_LENGTH(m_sysid_char);
	return length == GPOS_WSZ_LENGTH(sysid.m_sysid_char) && 0 == clib::Wcsncmp(m_sysid_char, sysid.m_sysid_char, length);
}

//---------------------------------------------------------------------------
//	@function:
//		CSystemId::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG CSystemId::HashValue() const
{
	return gpos::HashByteArray((BYTE *) m_sysid_char, GPOS_WSZ_LENGTH(m_sysid_char) * GPOS_SIZEOF(WCHAR));
}