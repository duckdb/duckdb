//---------------------------------------------------------------------------
//	@filename:
//		CMDIdScCmp.cpp
//
//	@doc:
//		Implementation of mdids for scalar comparisons functions
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/md/CMDIdScCmp.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::CMDIdScCmp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDIdScCmp::CMDIdScCmp(CMDIdGPDB *left_mdid, CMDIdGPDB *right_mdid,
					   IMDType::ECmpType cmp_type)
	: m_mdid_left(left_mdid),
	  m_mdid_right(right_mdid),
	  m_comparision_type(cmp_type),
	  m_str(m_mdid_array, GPOS_ARRAY_SIZE(m_mdid_array))
{
	GPOS_ASSERT(left_mdid->IsValid());
	GPOS_ASSERT(right_mdid->IsValid());
	GPOS_ASSERT(IMDType::EcmptOther != cmp_type);

	GPOS_ASSERT(left_mdid->Sysid().Equals(right_mdid->Sysid()));

	// serialize mdid into static string
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::~CMDIdScCmp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIdScCmp::~CMDIdScCmp()
{
	m_mdid_left->Release();
	m_mdid_right->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void
CMDIdScCmp::Serialize()
{
	// serialize mdid as SystemType.mdidLeft;mdidRight;CmpType
	m_str.AppendFormat(GPOS_WSZ_LIT("%d.%d.%d.%d;%d.%d.%d;%d"), MdidType(),
					   m_mdid_left->Oid(), m_mdid_left->VersionMajor(),
					   m_mdid_left->VersionMinor(), m_mdid_right->Oid(),
					   m_mdid_right->VersionMajor(),
					   m_mdid_right->VersionMinor(), m_comparision_type);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::GetBuffer
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const WCHAR *
CMDIdScCmp::GetBuffer() const
{
	return m_str.GetBuffer();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::GetLeftMdid
//
//	@doc:
//		Returns the source type id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdScCmp::GetLeftMdid() const
{
	return m_mdid_left;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::GetRightMdid
//
//	@doc:
//		Returns the destination type id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdScCmp::GetRightMdid() const
{
	return m_mdid_right;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::HashValue
//
//	@doc:
//		Computes the hash value for the metadata id
//
//---------------------------------------------------------------------------
ULONG
CMDIdScCmp::HashValue() const
{
	return gpos::CombineHashes(MdidType(),
							   gpos::CombineHashes(m_mdid_left->HashValue(),
												   m_mdid_right->HashValue()));
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::Equals
//
//	@doc:
//		Checks if the mdids are equal
//
//---------------------------------------------------------------------------
BOOL
CMDIdScCmp::Equals(const IMDId *mdid) const
{
	if (NULL == mdid || EmdidScCmp != mdid->MdidType())
	{
		return false;
	}

	const CMDIdScCmp *pmdidScCmp = CMDIdScCmp::CastMdid(mdid);

	return m_mdid_left->Equals(pmdidScCmp->GetLeftMdid()) &&
		   m_mdid_right->Equals(pmdidScCmp->GetRightMdid()) &&
		   m_comparision_type == pmdidScCmp->ParseCmpType();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream &
CMDIdScCmp::OsPrint(IOstream &os) const
{
	os << "(" << m_str.GetBuffer() << ")";
	return os;
}