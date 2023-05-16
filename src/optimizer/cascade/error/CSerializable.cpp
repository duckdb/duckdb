//---------------------------------------------------------------------------
//	@filename:
//		CSerializable.cpp
//
//	@doc:
//		Interface for serializable objects
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/error/CSerializable.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/error/CErrorContext.h"
#include "duckdb/optimizer/cascade/task/CTask.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CSerializable::CSerializable
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSerializable::CSerializable()
{
	CTask *task = CTask::Self();
	GPOS_ASSERT(NULL != task);
	task->ConvertErrCtxt()->Register(this);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializable::~CSerializable
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSerializable::~CSerializable()
{
	CTask *task = CTask::Self();
	GPOS_ASSERT(NULL != task);
	task->ConvertErrCtxt()->Unregister(this);
}