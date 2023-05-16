//---------------------------------------------------------------------------
//	@filename:
//		CIOUtils.cpp
//
//	@doc:
//		Implementation of optimizer I/O utilities
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/base/CIOUtils.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/io/COstreamFile.h"
#include "duckdb/optimizer/cascade/task/CAutoSuspendAbort.h"
#include "duckdb/optimizer/cascade/task/CWorker.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CIOUtils::Dump
//
//	@doc:
//		Dump given string to output file
//
//---------------------------------------------------------------------------
void
CIOUtils::Dump(CHAR *file_name, CHAR *sz)
{
	CAutoSuspendAbort asa;
	const ULONG ulWrPerms = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;

	GPOS_TRY
	{
		CFileWriter fw;
		fw.Open(file_name, ulWrPerms);
		const BYTE *pb = reinterpret_cast<const BYTE *>(sz);
		ULONG_PTR ulpLength = (ULONG_PTR) clib::Strlen(sz);
		fw.Write(pb, ulpLength);
		fw.Close();
	}
	GPOS_CATCH_EX(ex)
	{
		// ignore exceptions during dumping
		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;
}