//---------------------------------------------------------------------------
//	@filename:
//		CMDIdCast.cpp
//
//	@doc:
//		Implementation of mdids for cast functions
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/CMDIdCast.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::CMDIdCast
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDIdCast::CMDIdCast(CMDIdGPDB *mdid_src, CMDIdGPDB *mdid_dest)
	: m_mdid_src(mdid_src), m_mdid_dest(mdid_dest), m_str(m_mdid_buffer, GPOS_ARRAY_SIZE(m_mdid_buffer))
{
	GPOS_ASSERT(mdid_src->IsValid());
	GPOS_ASSERT(mdid_dest->IsValid());

	// serialize mdid into static string
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::~CMDIdCast
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIdCast::~CMDIdCast()
{
	m_mdid_src->Release();
	m_mdid_dest->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void
CMDIdCast::Serialize()
{
	// serialize mdid as SystemType.mdidSrc.mdidDest
	m_str.AppendFormat(GPOS_WSZ_LIT("%d.%d.%d.%d;%d.%d.%d"), MdidType(),
					   m_mdid_src->Oid(), m_mdid_src->VersionMajor(),
					   m_mdid_src->VersionMinor(), m_mdid_dest->Oid(),
					   m_mdid_dest->VersionMajor(),
					   m_mdid_dest->VersionMinor());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::GetBuffer
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const WCHAR *
CMDIdCast::GetBuffer() const
{
	return m_str.GetBuffer();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::MdidSrc
//
//	@doc:
//		Returns the source type id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdCast::MdidSrc() const
{
	return m_mdid_src;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::MdidDest
//
//	@doc:
//		Returns the destination type id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdCast::MdidDest() const
{
	return m_mdid_dest;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::Equals
//
//	@doc:
//		Checks if the mdids are equal
//
//---------------------------------------------------------------------------
BOOL
CMDIdCast::Equals(const IMDId *mdid) const
{
	if (NULL == mdid || EmdidCastFunc != mdid->MdidType())
	{
		return false;
	}

	const CMDIdCast *mdid_cast_func = CMDIdCast::CastMdid(mdid);

	return m_mdid_src->Equals(mdid_cast_func->MdidSrc()) &&
		   m_mdid_dest->Equals(mdid_cast_func->MdidDest());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream &
CMDIdCast::OsPrint(IOstream &os) const
{
	os << "(" << m_str.GetBuffer() << ")";
	return os;
}