//---------------------------------------------------------------------------
//	@filename:
//		COstreamFile.h
//
//	@doc:
//		Output file stream class;
//---------------------------------------------------------------------------
#ifndef GPOS_COstreamFile_H
#define GPOS_COstreamFile_H

#include "duckdb/optimizer/cascade/io/CFileWriter.h"
#include "duckdb/optimizer/cascade/io/COstream.h"
#include "duckdb/optimizer/cascade/io/ioutils.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		COstreamFile
//
//	@doc:
//		Implements an output stream writing to a file
//
//---------------------------------------------------------------------------
class COstreamFile : public COstream
{
private:
	// underlying file writer
	CFileWriter m_file_writer;
	// private copy ctor
	COstreamFile(const COstreamFile &);

public:
	// please see comments in COstream.h for an explanation
	using COstream::operator<<;
	// ctor
	COstreamFile(const CHAR *file_path, ULONG permission_bits = S_IRUSR | S_IWUSR);

	// dtor
	virtual ~COstreamFile();

	// implement << operator
	virtual IOstream &operator<<(const WCHAR *);
};

}  // namespace gpos

#endif