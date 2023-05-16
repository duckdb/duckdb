//---------------------------------------------------------------------------
//	@filename:
//		CMDIdColStats.cpp
//
//	@doc:
//		Implementation of mdids for column statistics
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/CMDIdColStats.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::CMDIdColStats
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDIdColStats::CMDIdColStats(CMDIdGPDB *rel_mdid, ULONG pos)
	: m_rel_mdid(rel_mdid), m_attr_pos(pos), m_str(m_mdid_buffer, GPOS_ARRAY_SIZE(m_mdid_buffer))
{
	GPOS_ASSERT(rel_mdid->IsValid());
	// serialize mdid into static string
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::~CMDIdColStats
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIdColStats::~CMDIdColStats()
{
	m_rel_mdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void CMDIdColStats::Serialize()
{
	// serialize mdid as SystemType.Oid.Major.Minor.Attno
	m_str.AppendFormat(GPOS_WSZ_LIT("%d.%d.%d.%d.%d"), MdidType(), m_rel_mdid->Oid(), m_rel_mdid->VersionMajor(), m_rel_mdid->VersionMinor(), m_attr_pos);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::GetBuffer
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const WCHAR* CMDIdColStats::GetBuffer() const
{
	return m_str.GetBuffer();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::GetRelMdId
//
//	@doc:
//		Returns the base relation id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdColStats::GetRelMdId() const
{
	return m_rel_mdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::Position
//
//	@doc:
//		Returns the attribute number
//
//---------------------------------------------------------------------------
ULONG
CMDIdColStats::Position() const
{
	return m_attr_pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::Equals
//
//	@doc:
//		Checks if the mdids are equal
//
//---------------------------------------------------------------------------
BOOL
CMDIdColStats::Equals(const IMDId *mdid) const
{
	if (NULL == mdid || EmdidColStats != mdid->MdidType())
	{
		return false;
	}

	const CMDIdColStats *mdid_col_stats = CMDIdColStats::CastMdid(mdid);

	return m_rel_mdid->Equals(mdid_col_stats->GetRelMdId()) &&
		   m_attr_pos == mdid_col_stats->Position();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream & CMDIdColStats::OsPrint(IOstream &os) const
{
	os << "(" << m_str.GetBuffer() << ")";
	return os;
}