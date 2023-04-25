//---------------------------------------------------------------------------
//	@filename:
//		CAutoTrace.h
//
//	@doc:
//		Auto object for creating trace messages
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoTrace_H
#define GPOS_CAutoTrace_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/COstreamString.h"
#include "duckdb/optimizer/cascade/string/CWStringDynamic.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CAutoTrace
//
//	@doc:
//		Auto object for creating trace messages;
//		creates a stream over a dynamic string and uses it to print objects;
//		at destruction the string is written to the log as a trace msg;
//
//---------------------------------------------------------------------------
class CAutoTrace : public CStackObject
{
private:
	// dynamic string buffer
	CWStringDynamic m_wstr;

	// string stream
	COstreamString m_os;

	// private copy ctor
	CAutoTrace(const CAutoTrace &);

public:
	// ctor
	explicit CAutoTrace(CMemoryPool *mp);

	// dtor
	~CAutoTrace();

	// stream accessor
	IOstream &
	Os()
	{
		return m_os;
	}

};	// class CAutoTrace
}  // namespace gpos

#endif
