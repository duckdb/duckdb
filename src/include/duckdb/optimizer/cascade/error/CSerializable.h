//---------------------------------------------------------------------------
//	@filename:
//		CSerializable.h
//
//	@doc:
//		Interface for serializable objects;
//		used to dump objects when an exception is raised;
//---------------------------------------------------------------------------
#ifndef GPOS_CSerializable_H
#define GPOS_CSerializable_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/common/CList.h"

namespace gpos
{
//---------------------------------------------------------------------------
//	@class:
//		CSerializable
//
//	@doc:
//		Interface for serializable objects;
//
//---------------------------------------------------------------------------
class CSerializable : CStackObject
{
private:
	// private copy ctor
	CSerializable(const CSerializable &);

public:
	// ctor
	CSerializable();

	// dtor
	virtual ~CSerializable();

	// serialize object to passed stream
	virtual void Serialize(COstream &oos) = 0;

	// link for list in error context
	SLink m_err_ctxt_link;

};	// class CSerializable
}  // namespace gpos

#endif
