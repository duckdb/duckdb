//---------------------------------------------------------------------------
//	@filename:
//		CMDIdColStats.h
//
//	@doc:
//		Class for representing mdids for column statistics
//---------------------------------------------------------------------------
#ifndef GPMD_CMDIdColStats_H
#define GPMD_CMDIdColStats_H

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
//		CMDIdColStats
//
//	@doc:
//		Class for representing ids of column stats objects
//
//---------------------------------------------------------------------------
class CMDIdColStats : public IMDId
{
private:
	// mdid of base relation
	CMDIdGPDB *m_rel_mdid;

	// position of the attribute in the base relation
	ULONG m_attr_pos;

	// buffer for the serialized mdid
	WCHAR m_mdid_buffer[GPDXL_MDID_LENGTH];

	// string representation of the mdid
	CWStringStatic m_str;

	// private copy ctor
	CMDIdColStats(const CMDIdColStats &);

	// serialize mdid
	void Serialize();

public:
	// ctor
	CMDIdColStats(CMDIdGPDB *rel_mdid, ULONG attno);

	// dtor
	virtual ~CMDIdColStats();

	virtual EMDIdType
	MdidType() const
	{
		return EmdidColStats;
	}

	// string representation of mdid
	virtual const WCHAR *GetBuffer() const;

	// source system id
	virtual CSystemId
	Sysid() const
	{
		return m_rel_mdid->Sysid();
	}

	// accessors
	IMDId *GetRelMdId() const;
	ULONG Position() const;

	// equality check
	virtual BOOL Equals(const IMDId *mdid) const;

	// computes the hash value for the metadata id
	virtual ULONG
	HashValue() const
	{
		return gpos::CombineHashes(m_rel_mdid->HashValue(),
								   gpos::HashValue(&m_attr_pos));
	}

	// is the mdid valid
	virtual BOOL
	IsValid() const
	{
		return IMDId::IsValid(m_rel_mdid);
	}

	// debug print of the metadata id
	virtual IOstream &OsPrint(IOstream &os) const;

	// const converter
	static const CMDIdColStats *
	CastMdid(const IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid && EmdidColStats == mdid->MdidType());

		return dynamic_cast<const CMDIdColStats *>(mdid);
	}

	// non-const converter
	static CMDIdColStats *
	CastMdid(IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid && EmdidColStats == mdid->MdidType());

		return dynamic_cast<CMDIdColStats *>(mdid);
	}
};

}  // namespace gpmd

#endif