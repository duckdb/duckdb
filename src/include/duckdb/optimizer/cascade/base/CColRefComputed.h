//---------------------------------------------------------------------------
//	@filename:
//		CColRefComputed.h
//
//	@doc:
//		Column reference implementation for computed columns
//---------------------------------------------------------------------------
#ifndef GPOS_CColRefComputed_H
#define GPOS_CColRefComputed_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CDynamicPtrArray.h"
#include "duckdb/optimizer/cascade/common/CList.h"

#include "duckdb/optimizer/cascade/base/CColRef.h"
#include "duckdb/optimizer/cascade/metadata/CName.h"
#include "duckdb/optimizer/cascade/md/IMDType.h"

namespace gpopt
{
using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@class:
//		CColRefComputed
//
//	@doc:
//
//---------------------------------------------------------------------------
class CColRefComputed : public CColRef
{
private:
	// private copy ctor
	CColRefComputed(const CColRefComputed &);

public:
	// ctor
	CColRefComputed(const IMDType *pmdtype, INT type_modifier, ULONG id,
					const CName *pname);

	// dtor
	virtual ~CColRefComputed();

	virtual CColRef::Ecolreftype
	Ecrt() const
	{
		return CColRef::EcrtComputed;
	}

	// is column a system column?
	BOOL
	FSystemCol() const
	{
		// we cannot introduce system columns as computed column
		return false;
	}


};	// class CColRefComputed

}  // namespace gpopt

#endif
