//---------------------------------------------------------------------------
//	@filename:
//		IDatumInt2.h
//
//	@doc:
//		Base abstract class for int2 representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatumInt2_H
#define GPNAUCRATES_IDatumInt2_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/IDatum.h"

namespace gpnaucrates
{
//---------------------------------------------------------------------------
//	@class:
//		IDatumInt2
//
//	@doc:
//		Base abstract class for int2 representation
//
//---------------------------------------------------------------------------
class IDatumInt2 : public IDatum
{
private:
	// private copy ctor
	IDatumInt2(const IDatumInt2 &);

public:
	// ctor
	IDatumInt2(){};

	// dtor
	virtual ~IDatumInt2(){};

	// accessor for datum type
	virtual IMDType::ETypeInfo
	GetDatumType()
	{
		return IMDType::EtiInt2;
	}

	// accessor of integer value
	virtual SINT Value() const = 0;

	// can datum be mapped to a double
	BOOL
	IsDatumMappableToDouble() const
	{
		return true;
	}

	// map to double for stats computation
	CDouble
	GetDoubleMapping() const
	{
		return CDouble(Value());
	}

	// can datum be mapped to LINT
	BOOL
	IsDatumMappableToLINT() const
	{
		return true;
	}

	// map to LINT for statistics computation
	LINT
	GetLINTMapping() const
	{
		return LINT(Value());
	}

	// byte array representation of datum
	virtual const BYTE *
	GetByteArrayValue() const
	{
		GPOS_ASSERT(!"Invalid invocation of MakeCopyOfValue");
		return NULL;
	}

	// does the datum need to be padded before statistical derivation
	virtual BOOL
	NeedsPadding() const
	{
		return false;
	}

	// return the padded datum
	virtual IDatum *
	MakePaddedDatum(CMemoryPool *,	// mp,
					ULONG			// col_len
	) const
	{
		GPOS_ASSERT(!"Invalid invocation of MakePaddedDatum");
		return NULL;
	}

	// does datum support like predicate
	virtual BOOL
	SupportsLikePredicate() const
	{
		return false;
	}

	// return the default scale factor of like predicate
	virtual CDouble
	GetLikePredicateScaleFactor() const
	{
		GPOS_ASSERT(!"Invalid invocation of DLikeSelectivity");
		return false;
	}

};	// class IDatumInt2

}  // namespace gpnaucrates

#endif