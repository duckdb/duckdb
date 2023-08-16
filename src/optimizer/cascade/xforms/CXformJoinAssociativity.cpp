//---------------------------------------------------------------------------
//	@filename:
//		CXformJoinCommutativity.cpp
//
//	@doc:
//		Implementation of join commute transform
//---------------------------------------------------------------------------
#include "duckdb/optimizer/cascade/xforms/CXformJoinAssociativity.h"
#include "duckdb/optimizer/cascade/base.h"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/optimizer/cascade/operators/CPatternLeaf.h"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/optimizer/cascade/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformJoinAssociativity::CXformJoinAssociativity
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformJoinAssociativity::CXformJoinAssociativity()
	: CXformExploration(make_uniq<LogicalComparisonJoin>(JoinType::INNER))
{
    duckdb::unique_ptr<LogicalComparisonJoin> left_child = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    left_child->AddChild(make_uniq<CPatternLeaf>());
    left_child->AddChild(make_uniq<CPatternLeaf>());
	this->m_operator->AddChild(std::move(left_child));
	this->m_operator->AddChild(make_uniq<CPatternLeaf>());
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::XformPromise
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise CXformJoinAssociativity::XformPromise(CExpressionHandle &exprhdl) const {
	return CXform::ExfpMedium;
}

void CXformJoinAssociativity::CreatePredicates(Operator* join, duckdb::vector<JoinCondition> &upper_join_condition,
                                            duckdb::vector<JoinCondition> &lower_join_condition) const {
    LogicalComparisonJoin* UpperJoin = (LogicalComparisonJoin*)join;
    LogicalComparisonJoin* LowerJoin = (LogicalComparisonJoin*)UpperJoin->children[0].get();
    duckdb::vector<ColumnBinding> NewLowerOutputCols;
    duckdb::vector<ColumnBinding> LowerLeftOutputCols = LowerJoin->children[0]->GetColumnBindings();
    duckdb::vector<ColumnBinding> UpperRightOutputCols = UpperJoin->children[1]->GetColumnBindings();
    NewLowerOutputCols.insert(NewLowerOutputCols.end(), LowerLeftOutputCols.begin(), LowerLeftOutputCols.end());
    NewLowerOutputCols.insert(NewLowerOutputCols.end(), UpperRightOutputCols.begin(), UpperRightOutputCols.end());
    for (auto &child : UpperJoin->conditions) {
        if (CUtils::ContainsAll(NewLowerOutputCols, child.left->getColumnBinding())
            && CUtils::ContainsAll(NewLowerOutputCols, child.right->getColumnBinding())) {
            JoinCondition jc;
            jc.left = child.left->Copy();
            jc.right = child.right->Copy();
            jc.comparison = child.comparison;
            lower_join_condition.emplace_back(std::move(jc));
        }
    }
    for (auto &child : UpperJoin->conditions) {
        if (CUtils::ContainsAll(NewLowerOutputCols, child.left->getColumnBinding())
            && CUtils::ContainsAll(NewLowerOutputCols, child.right->getColumnBinding())) {
            continue;
        }
        JoinCondition jc;
        jc.left = child.right->Copy();
        jc.right = child.left->Copy();
        jc.comparison = child.comparison;
        upper_join_condition.emplace_back(std::move(jc));
    }
    for (auto &child : LowerJoin->conditions) {
        JoinCondition jc;
        jc.left = child.left->Copy();
        jc.right = child.right->Copy();
        jc.comparison = child.comparison;
        upper_join_condition.emplace_back(std::move(jc));
    }
}

//---------------------------------------------------------------------------
//	@function:
//		CXformGet2TableScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void CXformJoinAssociativity::Transform(CXformContext* pxfctxt, CXformResult* pxfres, Operator* pexpr) const {
	LogicalComparisonJoin* UpperJoin = (LogicalComparisonJoin*)pexpr;
    LogicalComparisonJoin* LowerJoin = (LogicalComparisonJoin*)pexpr->children[0].get();
    duckdb::vector<JoinCondition> NewUpperJoinCondition;
    duckdb::vector<JoinCondition> NewLowerJoinCondition;
    CreatePredicates(UpperJoin, NewUpperJoinCondition, NewLowerJoinCondition);
    if(NewUpperJoinCondition.size() == 0 || NewLowerJoinCondition.size() == 0)
        return;
    duckdb::unique_ptr<LogicalComparisonJoin> NewLowerJoin = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    NewLowerJoin->AddChild(LowerJoin->children[0]->Copy());
    NewLowerJoin->AddChild(UpperJoin->children[1]->Copy());
    NewLowerJoin->conditions = std::move(NewLowerJoinCondition);
    NewLowerJoin->ResolveTypes();
    duckdb::unique_ptr<LogicalComparisonJoin> NewUpperJoin = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
    NewUpperJoin->AddChild(std::move(NewLowerJoin));
    NewUpperJoin->AddChild(LowerJoin->children[1]->Copy());
    NewUpperJoin->conditions = std::move(NewUpperJoinCondition);
    NewUpperJoin->ResolveTypes();
	// add alternative to transformation result
	pxfres->Add(std::move(NewUpperJoin));
}