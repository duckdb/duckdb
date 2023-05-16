//---------------------------------------------------------------------------
//	@filename:
//		IDatumBool.h
//
//	@doc:
//		Base abstract class for bool representation
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_IDatumBool_H
#define GPNAUCRATES_IDatumBool_H

#include "duckdb/optimizer/cascade/base.h"

#include "duckdb/optimizer/cascade/base/IDatum.h"

namespace gpnaucrates
{
//---------------------------------------------------------------------------
//	@class:
//		IDatumBool
//
//	@doc:
//		Base abstract class for bool representation
//
//---------------------------------------------------------------------------
class IDatumBool : public IDatum
{
private:
	// private copy ctor
	IDatumBool(const IDatumBool &);

public:
	// ctor
	IDatumBool(){};

	// dtor
	virtual ~IDatumBool(){};

	// accessor for datum type
	virtual IMDType::ETypeInfo
	GetDatumType()
	{
		return IMDType::EtiBool;
	}

	// accessor of boolean value
	virtual BOOL GetValue() const = 0;

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
		if (GetValue())
		{
			return CDouble(1.0);
		}

		return CDouble(0.0);
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
		if (GetValue())
		{
			return LINT(1);
		}
		return LINT(0);
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
};	// class IDatumBool

}  // namespace gpnaucrates

#endif