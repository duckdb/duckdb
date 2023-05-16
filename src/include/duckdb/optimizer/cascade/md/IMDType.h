//---------------------------------------------------------------------------
//	@filename:
//		IMDType.h
//
//	@doc:
//		Interface for types in the metadata cache
//---------------------------------------------------------------------------
#ifndef GPMD_IMDCacheType_H
#define GPMD_IMDCacheType_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/md/IMDCacheObject.h"

namespace gpnaucrates
{
class IDatum;
}

namespace gpmd
{
using namespace gpos;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@class:
//		IMDType
//
//	@doc:
//		Interface for types in the metadata cache
//
//---------------------------------------------------------------------------
class IMDType : public IMDCacheObject
{
public:
	enum ETypeInfo
	{
		EtiInt2,
		EtiInt4,
		EtiInt8,
		EtiBool,
		EtiOid,
		EtiGeneric	// should be the last in this enum
	};

	// comparison type
	enum ECmpType
	{
		EcmptEq,   // equals
		EcmptNEq,  // not equals
		EcmptL,	   // less than
		EcmptLEq,  // less or equal to
		EcmptG,	   // greater than
		EcmptGEq,  // greater or equal to
		EcmptIDF,  // is distinct from
		EcmptOther
	};

	// aggregate type
	enum EAggType
	{
		EaggMin,
		EaggMax,
		EaggAvg,
		EaggSum,
		EaggCount,
		EaggGeneric
	};

	// object type
	virtual Emdtype
	MDType() const
	{
		return EmdtType;
	}

	// md id of cache object
	virtual IMDId *MDId() const = 0;

	// id of specified specified comparison operator type
	virtual IMDId *GetMdidForCmpType(ECmpType cmp_type) const = 0;

	// id of specified specified aggregate type
	virtual IMDId *GetMdidForAggType(EAggType agg_type) const = 0;

	// id of comparison operator for type used in btree lookups
	virtual const IMDId *CmpOpMdid() const = 0;

	// id of hash operator for type
	virtual BOOL IsHashable() const = 0;

	// type is merge joinable on '='
	virtual BOOL IsMergeJoinable() const = 0;

	// is type redistributable
	virtual BOOL IsRedistributable() const = 0;

	// id of the array type for the type
	virtual IMDId *GetArrayTypeMdid() const = 0;

	// type id
	virtual ETypeInfo GetDatumType() const = 0;

	// is type fixed length
	virtual BOOL IsFixedLength() const = 0;

	// is type composite
	virtual BOOL IsComposite() const = 0;

	virtual BOOL
	IsTextRelated() const
	{
		return false;
	}

	// id of the relation corresponding to a composite type
	virtual IMDId *GetBaseRelMdid() const = 0;

	// type length
	virtual ULONG Length() const = 0;

	// is type passed by value
	virtual BOOL IsPassedByValue() const = 0;

	// return the null constant for this type
	virtual IDatum *DatumNull() const = 0;

	// is type an ambiguous one? e.g., AnyElement in GPDB
	virtual BOOL IsAmbiguous() const
	{
		return false;
	}

	// return true if we can perform statistical comparison between datums of these two types; else return false
	static BOOL StatsAreComparable(const IMDType *mdtype_first, const IMDType *mdtype_second);

	// return true if we can perform statistical comparison between datum of the given type and a given datum; else return false
	static BOOL StatsAreComparable(const IMDType *mdtype_first, const IDatum *datum_second);
};
}  // namespace gpmd

#endif
