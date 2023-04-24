//---------------------------------------------------------------------------
//	@filename:
//		CName.h
//
//	@doc:
//		Name abstraction for metadata names to keep optimizer
//		agnostic of encodings etc.
//---------------------------------------------------------------------------
#ifndef GPOPT_CName_H
#define GPOPT_CName_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/string/CWStringConst.h"

#define GPOPT_NAME_QUOTE_BEGIN "\""
#define GPOPT_NAME_QUOTE_END "\""

#define GPOPT_NAME_SEPARATOR GPOS_WSZ_LIT(".")

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CName
//
//	@doc:
//		Names consist of a null terminated wide character string;
//		No assumptions about format and encoding; only semantics
//		enforced is zero termination of string;
//
//---------------------------------------------------------------------------
class CName
{
private:
	// actual name
	const CWStringConst *m_str_name;

	// keep track of copy status
	BOOL m_fDeepCopy;

	// deep copy function
	void DeepCopy(CMemoryPool *mp, const CWStringConst *str);

public:
	// ctors
	CName(CMemoryPool *, const CWStringBase *);
	CName(const CWStringConst *, BOOL fOwnsMemory = false);
	CName(const CName &);

	CName(CMemoryPool *mp, const CName &);
	CName(CMemoryPool *mp, const CName &, const CName &);

	// dtor
	~CName();

	// accessors
	const CWStringConst *
	Pstr() const
	{
		return m_str_name;
	}

	ULONG
	Length() const
	{
		return m_str_name->Length();
	}

	// comparison
	BOOL Equals(const CName &) const;

	// debug print
	IOstream &OsPrint(IOstream &) const;

};	// class CName
}  // namespace gpopt

#endif
