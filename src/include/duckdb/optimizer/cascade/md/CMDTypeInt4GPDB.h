//---------------------------------------------------------------------------
//	@filename:
//		CMDTypeInt4GPDB.h
//
//	@doc:
//		Class for representing INT4 types in GPDB
//---------------------------------------------------------------------------
#ifndef GPMD_CMDTypeInt4GPDB_H
#define GPMD_CMDTypeInt4GPDB_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/md/IMDTypeInt4.h"

#define GPDB_INT4_OID OID(23)
#define GPDB_INT4_LENGTH 4
#define GPDB_INT4_EQ_OP OID(96)
#define GPDB_INT4_NEQ_OP OID(518)
#define GPDB_INT4_LT_OP OID(97)
#define GPDB_INT4_LEQ_OP OID(523)
#define GPDB_INT4_GT_OP OID(521)
#define GPDB_INT4_GEQ_OP OID(525)
#define GPDB_INT4_COMP_OP OID(351)
#define GPDB_INT4_EQ_FUNC OID(65)
#define GPDB_INT4_ARRAY_TYPE OID(1007)

#define GPDB_INT4_AGG_MIN OID(2132)
#define GPDB_INT4_AGG_MAX OID(2116)
#define GPDB_INT4_AGG_AVG OID(2101)
#define GPDB_INT4_AGG_SUM OID(2108)
#define GPDB_INT4_AGG_COUNT OID(2147)

// fwd decl
namespace gpdxl
{
class CXMLSerializer;
}

namespace gpnaucrates
{
class IDatumInt4;
}

namespace gpmd
{
using namespace gpos;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@class:
//		CMDTypeInt4GPDB
//
//	@doc:
//		Class for representing INT4 types in GPDB
//
//---------------------------------------------------------------------------
class CMDTypeInt4GPDB : public IMDTypeInt4
{
private:
	// memory pool
	CMemoryPool *m_mp;

	// type id
	IMDId *m_mdid;

	// mdids of different operators
	IMDId *m_mdid_op_eq;
	IMDId *m_mdid_op_neq;
	IMDId *m_mdid_op_lt;
	IMDId *m_mdid_op_leq;
	IMDId *m_mdid_op_gt;
	IMDId *m_mdid_op_geq;
	IMDId *m_mdid_op_cmp;
	IMDId *m_mdid_type_array;

	// min aggregate
	IMDId *m_mdid_min;

	// max aggregate
	IMDId *m_mdid_max;

	// avg aggregate
	IMDId *m_mdid_avg;

	// sum aggregate
	IMDId *m_mdid_sum;

	// count aggregate
	IMDId *m_mdid_count;

	// DXL for object
	const CWStringDynamic *m_dxl_str;

	// type name and type
	static CWStringConst m_str;
	static CMDName m_mdname;

	// a null datum of this type (used for statistics comparison)
	IDatum *m_datum_null;

	// private copy ctor
	CMDTypeInt4GPDB(const CMDTypeInt4GPDB &);

public:
	// ctor
	explicit CMDTypeInt4GPDB(CMemoryPool *mp);

	//dtor
	virtual ~CMDTypeInt4GPDB();

	// factory method for creating INT4 datums
	virtual IDatumInt4 *CreateInt4Datum(CMemoryPool *mp, INT iValue,
										BOOL is_null) const;

	// accessors
	virtual const CWStringDynamic *
	GetStrRepr() const
	{
		return m_dxl_str;
	}

	virtual IMDId *MDId() const;

	virtual CMDName Mdname() const;

	// id of specified comparison operator type
	virtual IMDId *GetMdidForCmpType(ECmpType cmp_type) const;

	// id of specified specified aggregate type
	virtual IMDId *GetMdidForAggType(EAggType agg_type) const;

	virtual BOOL
	IsRedistributable() const
	{
		return true;
	}

	virtual BOOL
	IsFixedLength() const
	{
		return true;
	}

	// is type composite
	virtual BOOL
	IsComposite() const
	{
		return false;
	}

	virtual ULONG
	Length() const
	{
		return GPDB_INT4_LENGTH;
	}

	// return the GPDB length
	virtual INT
	GetGPDBLength() const
	{
		return GPDB_INT4_LENGTH;
	}

	virtual BOOL
	IsPassedByValue() const
	{
		return true;
	}

	virtual const IMDId *
	CmpOpMdid() const
	{
		return m_mdid_op_cmp;
	}

	// is type hashable
	virtual BOOL
	IsHashable() const
	{
		return true;
	}

	// is type merge joinable
	virtual BOOL
	IsMergeJoinable() const
	{
		return true;
	}

	virtual IMDId *
	GetArrayTypeMdid() const
	{
		return m_mdid_type_array;
	}

	// id of the relation corresponding to a composite type
	virtual IMDId *
	GetBaseRelMdid() const
	{
		return NULL;
	}

	// serialize object in DXL format
	virtual void Serialize(gpdxl::CXMLSerializer *xml_serializer) const;

	// return the null constant for this type
	virtual IDatum *
	DatumNull() const
	{
		return m_datum_null;
	}

	// transformation method for generating datum from CDXLScalarConstValue
	virtual IDatum *GetDatumForDXLConstVal(
		const CDXLScalarConstValue *dxl_op) const;

	// create typed datum from DXL datum
	virtual IDatum *GetDatumForDXLDatum(CMemoryPool *mp,
										const CDXLDatum *dxl_datum) const;

	// generate the DXL datum from IDatum
	virtual CDXLDatum *GetDatumVal(CMemoryPool *mp, IDatum *datum) const;

	// generate the DXL datum representing null value
	virtual CDXLDatum *GetDXLDatumNull(CMemoryPool *mp) const;

	// generate the DXL scalar constant from IDatum
	virtual CDXLScalarConstValue *GetDXLOpScConst(CMemoryPool *mp,
												  IDatum *datum) const;

#ifdef GPOS_DEBUG
	// debug print of the type in the provided stream
	virtual void DebugPrint(IOstream &os) const;
#endif
};
}  // namespace gpmd

#endif
