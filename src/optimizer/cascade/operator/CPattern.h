//---------------------------------------------------------------------------
//	@filename:
//		CPattern.h
//
//	@doc:
//		Base class for all pattern operators
//---------------------------------------------------------------------------
#ifndef GPOPT_CPattern_H
#define GPOPT_CPattern_H

#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/optimizer/cascade/operators/Operator.h"

namespace gpopt
{
using namespace gpos;
using namespace duckdb;

//---------------------------------------------------------------------------
//	@class:
//		CPattern
//
//	@doc:
//		base class for all pattern operators
//
//---------------------------------------------------------------------------
class CPattern : public Operator
{
public:
	// ctor
	explicit CPattern()
		: Operator()
	{
		logical_type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
		physical_type = PhysicalOperatorType::EXTENSION;
	}

	// private copy ctor
	CPattern(const CPattern &) = delete;

	// dtor
	virtual ~CPattern()
	{
	}

	// type of operator
	virtual bool FPattern() const override
	{
		return true;
	}

	// create derived properties container
	CDrvdProp* PdpCreate() override;

	// create required properties container
	CReqdProp* PrpCreate() const override;

	// match function
	bool Matches(Operator* op) override;

	// sensitivity to order of inputs
	bool FInputOrderSensitive() override;

	// check if operator is a pattern leaf
	virtual bool FLeaf() const = 0;

	// return a copy of the operator with remapped columns
	virtual Operator* PopCopyWithRemappedColumns(std::map<ULONG, ColumnBinding> colref_mapping, bool must_exist);

	// conversion function
	static CPattern* PopConvert(Operator* pop)
	{
		return reinterpret_cast<CPattern*>(pop);
	}

	duckdb::vector<ColumnBinding> GetColumnBindings() override
	{
		duckdb::vector<ColumnBinding> v;
		return v;
	}

	CKeyCollection* DeriveKeyCollection(CExpressionHandle &exprhdl) override
	{
		return nullptr;
	}

	CPropConstraint* DerivePropertyConstraint(CExpressionHandle &exprhdl) override
	{
		return nullptr;
	}

	ULONG DeriveJoinDepth(CExpressionHandle &exprhdl) override
	{
		return 0;
	}

	Operator* SelfRehydrate(CCostContext* pcc, duckdb::vector<Operator*> pdrgpexpr, CDrvdPropCtxtPlan* pdpctxtplan) override
	{
		return nullptr;
	}
};	// class CPattern
}  // namespace gpopt
#endif