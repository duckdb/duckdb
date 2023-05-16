//---------------------------------------------------------------------------
//	@filename:
//		CMDIdRelStats.h
//
//	@doc:
//		Class for representing mdids for relation statistics
//---------------------------------------------------------------------------
#ifndef GPMD_CMDIdRelStats_H
#define GPMD_CMDIdRelStats_H

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
//		CMDIdRelStats
//
//	@doc:
//		Class for representing ids of relation stats objects
//
//---------------------------------------------------------------------------
class CMDIdRelStats : public IMDId
{
private:
	// mdid of base relation
	CMDIdGPDB *m_rel_mdid;

	// buffer for the serialzied mdid
	WCHAR m_mdid_array[GPDXL_MDID_LENGTH];

	// string representation of the mdid
	CWStringStatic m_str;

	// private copy ctor
	CMDIdRelStats(const CMDIdRelStats &);

	// serialize mdid
	void Serialize();

public:
	// ctor
	explicit CMDIdRelStats(CMDIdGPDB *rel_mdid);

	// dtor
	virtual ~CMDIdRelStats();

	virtual EMDIdType
	MdidType() const
	{
		return EmdidRelStats;
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

	// equality check
	virtual BOOL Equals(const IMDId *mdid) const;

	// computes the hash value for the metadata id
	virtual ULONG
	HashValue() const
	{
		return m_rel_mdid->HashValue();
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
	static const CMDIdRelStats *
	CastMdid(const IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid && EmdidRelStats == mdid->MdidType());

		return dynamic_cast<const CMDIdRelStats *>(mdid);
	}

	// non-const converter
	static CMDIdRelStats *
	CastMdid(IMDId *mdid)
	{
		GPOS_ASSERT(NULL != mdid && EmdidRelStats == mdid->MdidType());

		return dynamic_cast<CMDIdRelStats *>(mdid);
	}
};

}  // namespace gpmd

#endif