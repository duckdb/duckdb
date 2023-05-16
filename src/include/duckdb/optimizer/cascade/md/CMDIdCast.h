//---------------------------------------------------------------------------
//	@filename:
//		CMDIdCast.h
//
//	@doc:
//		Class for representing mdids of cast functions
//---------------------------------------------------------------------------
#ifndef GPMD_CMDIdCastFunc_H
#define GPMD_CMDIdCastFunc_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/string/CWStringConst.h"
#include "duckdb/optimizer/cascade/md/CMDIdGPDB.h"
#include "duckdb/optimizer/cascade/md/CSystemId.h"

namespace gpmd
{
using namespace gpos;


//---------------------------------------------------------------------------
//	@class:
//		CMDIdCast
//
//	@doc:
//		Class for representing ids of cast objects
//
//---------------------------------------------------------------------------
class CMDIdCast : public IMDId
{
private:
	// mdid of source type
	CMDIdGPDB *m_mdid_src;

	// mdid of destinatin type
	CMDIdGPDB *m_mdid_dest;


	// buffer for the serialized mdid
	WCHAR m_mdid_buffer[GPDXL_MDID_LENGTH];

	// string representation of the mdid
	CWStringStatic m_str;

	// private copy ctor
	CMDIdCast(const CMDIdCast &);

	// serialize mdid
	void Serialize();

public:
	// ctor
	CMDIdCast(CMDIdGPDB *mdid_src, CMDIdGPDB *mdid_dest);

	// dtor
	virtual ~CMDIdCast();

	virtual EMDIdType
	MdidType() const
	{
		return EmdidCastFunc;
	}

	// string representation of mdid
	virtual const WCHAR *GetBuffer() const;

	// source system id
	virtual CSystemId
	Sysid() const
	{
		return m_mdid_src->Sysid();
	}

	// source type id
	IMDId *MdidSrc() const;

	// destination type id
	IMDId *MdidDest() const;

	// equality check
	virtual BOOL Equals(const IMDId *mdid) const;

	// computes the hash value for the metadata id
	virtual ULONG
	HashValue() const
	{
		return gpos::CombineHashes(
			MdidType(), gpos::CombineHashes(m_mdid_src->HashValue(),
											m_mdid_dest->HashValue()));
	}

	// is the mdid valid
	virtual BOOL
	IsValid() const
	{
		return IMDId::IsValid(m_mdid_src) && IMDId::IsValid(m_mdid_dest);
	}

	// debug print of the metadata id
	virtual IOstream &OsPrint(IOstream &os) const;

	// const converter
	static const CMDIdCast *
	CastMdid(const IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid && EmdidCastFunc == mdid->MdidType());

		return dynamic_cast<const CMDIdCast *>(mdid);
	}

	// non-const converter
	static CMDIdCast *
	CastMdid(IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid && EmdidCastFunc == mdid->MdidType());

		return dynamic_cast<CMDIdCast *>(mdid);
	}
};
}  // namespace gpmd

#endif
