//---------------------------------------------------------------------------
//	@filename:
//		COstreamBasic.h
//
//	@doc:
//		Output stream base class;
//---------------------------------------------------------------------------
#ifndef GPOS_COstreamBasic_H
#define GPOS_COstreamBasic_H

#include "duckdb/optimizer/cascade/io/COstream.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		COstreamBasic
//
//	@doc:
//		Implements a basic write thru interface over a std::WOSTREAM
//
//---------------------------------------------------------------------------

class COstreamBasic : public COstream
{
private:
	// underlying stream
	WOSTREAM *m_ostream;

	// private copy ctor
	COstreamBasic(const COstreamBasic &);

public:
	// please see comments in COstream.h for an explanation
	using COstream::operator<<;

	// ctor
	explicit COstreamBasic(WOSTREAM *ostream);

	virtual ~COstreamBasic()
	{
	}

	// implement << operator
	virtual IOstream &operator<<(const WCHAR *);

	// implement << operator
	virtual IOstream &operator<<(const WCHAR);
};

}  // namespace gpos

#endif