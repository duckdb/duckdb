//---------------------------------------------------------------------------
//	@filename:
//		CMDIdScCmp.h
//
//	@doc:
//		Class for representing mdids of scalar comparison operators
//---------------------------------------------------------------------------
#ifndef GPMD_CMDIdScCmpFunc_H
#define GPMD_CMDIdScCmpFunc_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

namespace gpmd
{
using namespace gpos;


//---------------------------------------------------------------------------
//	@class:
//		CMDIdScCmp
//
//	@doc:
//		Class for representing ids of scalar comparison operators
//
//---------------------------------------------------------------------------
class CMDIdScCmp : public IMDId
{
private:
	// mdid of source type
	CMDIdGPDB *m_mdid_left;

	// mdid of destinatin type
	CMDIdGPDB *m_mdid_right;

	// comparison type
	IMDType::ECmpType m_comparision_type;

	// buffer for the serialized mdid
	WCHAR m_mdid_array[GPDXL_MDID_LENGTH];

	// string representation of the mdid
	CWStringStatic m_str;

	// private copy ctor
	CMDIdScCmp(const CMDIdScCmp &);

	// serialize mdid
	void Serialize();

public:
	// ctor
	CMDIdScCmp(CMDIdGPDB *left_mdid, CMDIdGPDB *right_mdid,
			   IMDType::ECmpType cmp_type);

	// dtor
	virtual ~CMDIdScCmp();

	virtual EMDIdType
	MdidType() const
	{
		return EmdidScCmp;
	}

	// string representation of mdid
	virtual const WCHAR *GetBuffer() const;

	// source system id
	virtual CSystemId
	Sysid() const
	{
		return m_mdid_left->Sysid();
	}

	// left type id
	IMDId *GetLeftMdid() const;

	// right type id
	IMDId *GetRightMdid() const;

	IMDType::ECmpType
	ParseCmpType() const
	{
		return m_comparision_type;
	}

	// equality check
	virtual BOOL Equals(const IMDId *mdid) const;

	// computes the hash value for the metadata id
	virtual ULONG HashValue() const;

	// is the mdid valid
	virtual BOOL
	IsValid() const
	{
		return IMDId::IsValid(m_mdid_left) && IMDId::IsValid(m_mdid_right) &&
			   IMDType::EcmptOther != m_comparision_type;
	}

	// debug print of the metadata id
	virtual IOstream &OsPrint(IOstream &os) const;

	// const converter
	static const CMDIdScCmp *
	CastMdid(const IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid && EmdidScCmp == mdid->MdidType());

		return dynamic_cast<const CMDIdScCmp *>(mdid);
	}

	// non-const converter
	static CMDIdScCmp *
	CastMdid(IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid && EmdidScCmp == mdid->MdidType());

		return dynamic_cast<CMDIdScCmp *>(mdid);
	}
};
}  // namespace gpmd

#endif