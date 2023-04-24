//---------------------------------------------------------------------------
//	@filename:
//		COstream.h
//
//	@doc:
//		Partial implementation of output stream interface;
//---------------------------------------------------------------------------
#ifndef GPOS_COstream_H
#define GPOS_COstream_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/IOstream.h"
#include "duckdb/optimizer/cascade/string/CWStringStatic.h"

// conversion buffer size
#define GPOS_OSTREAM_CONVBUF_SIZE (256)

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		COstream
//
//	@doc:
//		Defines all available operator interfaces; avoids having to overload
//		system stream classes or their operators/member functions;
//		When inheriting from this class, C++ hides 'all' overloaded
//		versions of a function in the subclass, by default. Therefore, the
//		compiler will not be able to 'see' the default implementations of the <<
//		operator in subclasses of COstream. Use the 'using' keyword as in
//		COstreamBasic.h to avoid the problem. Also refer to
//		Effective C++ Third Edition, pp156
//
//---------------------------------------------------------------------------

class COstream : public IOstream
{
protected:
	// constructor
	COstream();

public:
	using IOstream::operator<<;

	// virtual dtor
	virtual ~COstream()
	{
	}

	// default implementations for the following interfaces available
	virtual IOstream &operator<<(const CHAR *);
	virtual IOstream &operator<<(const WCHAR);
	virtual IOstream &operator<<(const CHAR);
	virtual IOstream &operator<<(ULONG);
	virtual IOstream &operator<<(ULLONG);
	virtual IOstream &operator<<(INT);
	virtual IOstream &operator<<(LINT);
	virtual IOstream &operator<<(DOUBLE);
	virtual IOstream &operator<<(const void *);

	// to support std:endl only
	virtual IOstream &operator<<(WOSTREAM &(*) (WOSTREAM &) );

	// set the stream modifier
	virtual IOstream &operator<<(EStreamManipulator);

private:
	// formatting buffer
	WCHAR m_string_format_buffer[GPOS_OSTREAM_CONVBUF_SIZE];

	// wrapper string for formatting buffer
	CWStringStatic m_static_string_buffer;

	// current mode
	EStreamManipulator m_stream_manipulator;

	// append formatted string
	IOstream &AppendFormat(const WCHAR *format, ...);

	// what is the stream modifier?
	EStreamManipulator GetStreamManipulator() const;

	// no copy constructor
	COstream(COstream &);
};

}  // namespace gpos

#endif
