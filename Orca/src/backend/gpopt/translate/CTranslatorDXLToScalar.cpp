//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorDXLToScalar.cpp
//
//	@doc:
//		Implementation of the methods used to translate DXL Scalar Node into
//		GPDB's Expr.
//
//	@test:
//---------------------------------------------------------------------------

extern "C" {
#include "postgres.h"

#include "catalog/pg_collation.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "utils/datum.h"
}

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/gpdbwrappers.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"
#include "gpopt/translate/CMappingColIdVarPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToPlStmt.h"
#include "gpopt/translate/CTranslatorDXLToScalar.h"
#include "gpopt/translate/CTranslatorUtils.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLDatumBool.h"
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"
#include "naucrates/dxl/operators/CDXLDatumInt2.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/operators/CDXLDatumInt8.h"
#include "naucrates/dxl/operators/CDXLDatumOid.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/md/IMDAggregate.h"
#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDTypeBool.h"

using namespace gpdxl;
using namespace gpos;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::CTranslatorDXLToScalar
//
//	@doc:
//		Constructor
//---------------------------------------------------------------------------
CTranslatorDXLToScalar::CTranslatorDXLToScalar(CMemoryPool *mp,
											   CMDAccessor *md_accessor,
											   ULONG num_segments)
	: m_mp(mp),
	  m_md_accessor(md_accessor),
	  m_has_subqueries(false),
	  m_num_of_segments(num_segments)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLToScalar
//
//	@doc:
//		Translates a DXL scalar expression into a GPDB Expression node
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLToScalar(const CDXLNode *dxlnode,
											 CMappingColIdVar *colid_var)
{
	static const STranslatorElem translators[] = {
		{EdxlopScalarIdent,
		 &CTranslatorDXLToScalar::TranslateDXLScalarIdentToScalar},
		{EdxlopScalarCmp,
		 &CTranslatorDXLToScalar::TranslateDXLScalarCmpToScalar},
		{EdxlopScalarDistinct,
		 &CTranslatorDXLToScalar::TranslateDXLScalarDistinctToScalar},
		{EdxlopScalarOpExpr,
		 &CTranslatorDXLToScalar::TranslateDXLScalarOpExprToScalar},
		{EdxlopScalarArrayComp,
		 &CTranslatorDXLToScalar::TranslateDXLScalarArrayCompToScalar},
		{EdxlopScalarCoalesce,
		 &CTranslatorDXLToScalar::TranslateDXLScalarCoalesceToScalar},
		{EdxlopScalarMinMax,
		 &CTranslatorDXLToScalar::TranslateDXLScalarMinMaxToScalar},
		{EdxlopScalarConstValue,
		 &CTranslatorDXLToScalar::TranslateDXLScalarConstToScalar},
		{EdxlopScalarBoolExpr,
		 &CTranslatorDXLToScalar::TranslateDXLScalarBoolExprToScalar},
		{EdxlopScalarBooleanTest,
		 &CTranslatorDXLToScalar::TranslateDXLScalarBooleanTestToScalar},
		{EdxlopScalarNullTest,
		 &CTranslatorDXLToScalar::TranslateDXLScalarNullTestToScalar},
		{EdxlopScalarNullIf,
		 &CTranslatorDXLToScalar::TranslateDXLScalarNullIfToScalar},
		{EdxlopScalarIfStmt,
		 &CTranslatorDXLToScalar::TranslateDXLScalarIfStmtToScalar},
		{EdxlopScalarSwitch,
		 &CTranslatorDXLToScalar::TranslateDXLScalarSwitchToScalar},
		{EdxlopScalarCaseTest,
		 &CTranslatorDXLToScalar::TranslateDXLScalarCaseTestToScalar},
		{EdxlopScalarFuncExpr,
		 &CTranslatorDXLToScalar::TranslateDXLScalarFuncExprToScalar},
		{EdxlopScalarAggref,
		 &CTranslatorDXLToScalar::TranslateDXLScalarAggrefToScalar},
		{EdxlopScalarWindowRef,
		 &CTranslatorDXLToScalar::TranslateDXLScalarWindowRefToScalar},
		{EdxlopScalarCast,
		 &CTranslatorDXLToScalar::TranslateDXLScalarCastToScalar},
		{EdxlopScalarCoerceToDomain,
		 &CTranslatorDXLToScalar::TranslateDXLScalarCoerceToDomainToScalar},
		{EdxlopScalarCoerceViaIO,
		 &CTranslatorDXLToScalar::TranslateDXLScalarCoerceViaIOToScalar},
		{EdxlopScalarArrayCoerceExpr,
		 &CTranslatorDXLToScalar::TranslateDXLScalarArrayCoerceExprToScalar},
		{EdxlopScalarSubPlan,
		 &CTranslatorDXLToScalar::TranslateDXLScalarSubplanToScalar},
		{EdxlopScalarArray,
		 &CTranslatorDXLToScalar::TranslateDXLScalarArrayToScalar},
		{EdxlopScalarArrayRef,
		 &CTranslatorDXLToScalar::TranslateDXLScalarArrayRefToScalar},
		{EdxlopScalarDMLAction,
		 &CTranslatorDXLToScalar::TranslateDXLScalarDMLActionToScalar},
#if 0
		// GPDB_12_MERGE_FIXME: These were removed from the server with the v12 merge
		// of upstream partitioning. Need something to replace? Need to rip out from GPORCA?
		{EdxlopScalarPartDefault, &CTranslatorDXLToScalar::TranslateDXLScalarPartDefaultToScalar},
		{EdxlopScalarPartBound, &CTranslatorDXLToScalar::TranslateDXLScalarPartBoundToScalar},
		{EdxlopScalarPartBoundInclusion, &CTranslatorDXLToScalar::TranslateDXLScalarPartBoundInclusionToScalar},
		{EdxlopScalarPartBoundOpen, &CTranslatorDXLToScalar::TranslateDXLScalarPartBoundOpenToScalar},
		{EdxlopScalarPartListValues, &CTranslatorDXLToScalar::TranslateDXLScalarPartListValuesToScalar},
		{EdxlopScalarPartListNullTest, &CTranslatorDXLToScalar::TranslateDXLScalarPartListNullTestToScalar},
#endif
	};

	const ULONG num_translators = GPOS_ARRAY_SIZE(translators);

	GPOS_ASSERT(NULL != dxlnode);
	Edxlopid eopid = dxlnode->GetOperator()->GetDXLOperator();

	// find translator for the node type
	expr_func_ptr translate_func = NULL;
	for (ULONG ul = 0; ul < num_translators; ul++)
	{
		STranslatorElem elem = translators[ul];
		if (eopid == elem.eopid)
		{
			translate_func = elem.translate_func;
			break;
		}
	}

	if (NULL == translate_func)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
				   dxlnode->GetOperator()->GetOpNameStr()->GetBuffer());
	}

	return (this->*translate_func)(dxlnode, colid_var);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarIfStmtToScalar
//
//	@doc:
//		Translates a DXL scalar if stmt into a GPDB CaseExpr node
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarIfStmtToScalar(
	const CDXLNode *scalar_if_stmt_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_if_stmt_node);
	CDXLScalarIfStmt *scalar_if_stmt_dxl =
		CDXLScalarIfStmt::Cast(scalar_if_stmt_node->GetOperator());

	CaseExpr *case_expr = MakeNode(CaseExpr);
	case_expr->casetype =
		CMDIdGPDB::CastMdid(scalar_if_stmt_dxl->GetResultTypeMdId())->Oid();
	// GPDB_91_MERGE_FIXME: collation
	case_expr->casecollid = gpdb::TypeCollation(case_expr->casetype);

	CDXLNode *curr_node = const_cast<CDXLNode *>(scalar_if_stmt_node);
	Expr *else_expr = NULL;

	// An If statement is of the format: IF <condition> <then> <else>
	// The leaf else statement is the def result of the case statement
	BOOL is_leaf_else_stmt = false;

	while (!is_leaf_else_stmt)
	{
		if (3 != curr_node->Arity())
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLIncorrectNumberOfChildren);
			return NULL;
		}

		Expr *when_expr = TranslateDXLToScalar((*curr_node)[0], colid_var);
		Expr *then_expr = TranslateDXLToScalar((*curr_node)[1], colid_var);

		CaseWhen *case_when = MakeNode(CaseWhen);
		case_when->expr = when_expr;
		case_when->result = then_expr;
		case_expr->args = gpdb::LAppend(case_expr->args, case_when);

		if (EdxlopScalarIfStmt ==
			(*curr_node)[2]->GetOperator()->GetDXLOperator())
		{
			curr_node = (*curr_node)[2];
		}
		else
		{
			is_leaf_else_stmt = true;
			else_expr = TranslateDXLToScalar((*curr_node)[2], colid_var);
		}
	}

	case_expr->defresult = else_expr;

	return (Expr *) case_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarSwitchToScalar
//
//	@doc:
//		Translates a DXL scalar switch into a GPDB CaseExpr node
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarSwitchToScalar(
	const CDXLNode *scalar_switch_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_switch_node);
	CDXLScalarSwitch *dxlop =
		CDXLScalarSwitch::Cast(scalar_switch_node->GetOperator());

	CaseExpr *case_expr = MakeNode(CaseExpr);
	case_expr->casetype = CMDIdGPDB::CastMdid(dxlop->MdidType())->Oid();
	// GPDB_91_MERGE_FIXME: collation
	case_expr->casecollid = gpdb::TypeCollation(case_expr->casetype);

	// translate arg child
	case_expr->arg = TranslateDXLToScalar((*scalar_switch_node)[0], colid_var);
	GPOS_ASSERT(NULL != case_expr->arg);

	const ULONG arity = scalar_switch_node->Arity();
	GPOS_ASSERT(1 < arity);
	for (ULONG ul = 1; ul < arity; ul++)
	{
		const CDXLNode *child_dxl = (*scalar_switch_node)[ul];

		if (EdxlopScalarSwitchCase ==
			child_dxl->GetOperator()->GetDXLOperator())
		{
			CaseWhen *case_when = MakeNode(CaseWhen);
			case_when->expr = TranslateDXLToScalar((*child_dxl)[0], colid_var);
			case_when->result =
				TranslateDXLToScalar((*child_dxl)[1], colid_var);
			case_expr->args = gpdb::LAppend(case_expr->args, case_when);
		}
		else
		{
			// default return value
			GPOS_ASSERT(ul == arity - 1);
			case_expr->defresult =
				TranslateDXLToScalar((*scalar_switch_node)[ul], colid_var);
		}
	}

	return (Expr *) case_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarCaseTestToScalar
//
//	@doc:
//		Translates a DXL scalar case test into a GPDB CaseTestExpr node
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarCaseTestToScalar(
	const CDXLNode *scalar_case_test_node,
	CMappingColIdVar *	//colid_var
)
{
	GPOS_ASSERT(NULL != scalar_case_test_node);
	CDXLScalarCaseTest *dxlop =
		CDXLScalarCaseTest::Cast(scalar_case_test_node->GetOperator());

	CaseTestExpr *case_test_expr = MakeNode(CaseTestExpr);
	case_test_expr->typeId = CMDIdGPDB::CastMdid(dxlop->MdidType())->Oid();
	case_test_expr->typeMod = -1;
	// GPDB_91_MERGE_FIXME: collation
	case_test_expr->collation = gpdb::TypeCollation(case_test_expr->typeId);

	return (Expr *) case_test_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarOpExprToScalar
//
//	@doc:
//		Translates a DXL scalar opexpr into a GPDB OpExpr node
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarOpExprToScalar(
	const CDXLNode *scalar_op_expr_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_op_expr_node);
	CDXLScalarOpExpr *scalar_op_expr_dxl =
		CDXLScalarOpExpr::Cast(scalar_op_expr_node->GetOperator());

	OpExpr *op_expr = MakeNode(OpExpr);
	op_expr->opno = CMDIdGPDB::CastMdid(scalar_op_expr_dxl->MDId())->Oid();

	const IMDScalarOp *md_scalar_op =
		m_md_accessor->RetrieveScOp(scalar_op_expr_dxl->MDId());
	op_expr->opfuncid = CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();

	IMDId *return_type_mdid = scalar_op_expr_dxl->GetReturnTypeMdId();
	if (NULL != return_type_mdid)
	{
		op_expr->opresulttype = CMDIdGPDB::CastMdid(return_type_mdid)->Oid();
	}
	else
	{
		op_expr->opresulttype =
			GetFunctionReturnTypeOid(md_scalar_op->FuncMdId());
	}

	const IMDFunction *md_func =
		m_md_accessor->RetrieveFunc(md_scalar_op->FuncMdId());
	op_expr->opretset = md_func->ReturnsSet();

	GPOS_ASSERT(1 == scalar_op_expr_node->Arity() ||
				2 == scalar_op_expr_node->Arity());

	// translate children
	op_expr->args =
		TranslateScalarChildren(op_expr->args, scalar_op_expr_node, colid_var);

	// GPDB_91_MERGE_FIXME: collation
	op_expr->inputcollid = gpdb::ExprCollation((Node *) op_expr->args);
	op_expr->opcollid = gpdb::TypeCollation(op_expr->opresulttype);

	return (Expr *) op_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarArrayCompToScalar
//
//	@doc:
//		Translates a CDXLScalarArrayComp into a GPDB ScalarArrayOpExpr
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarArrayCompToScalar(
	const CDXLNode *scalar_array_comp_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_array_comp_node);
	CDXLScalarArrayComp *array_comp_dxl =
		CDXLScalarArrayComp::Cast(scalar_array_comp_node->GetOperator());

	ScalarArrayOpExpr *array_op_expr = MakeNode(ScalarArrayOpExpr);
	array_op_expr->opno = CMDIdGPDB::CastMdid(array_comp_dxl->MDId())->Oid();
	const IMDScalarOp *md_scalar_op =
		m_md_accessor->RetrieveScOp(array_comp_dxl->MDId());
	array_op_expr->opfuncid =
		CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();

	switch (array_comp_dxl->GetDXLArrayCmpType())
	{
		case Edxlarraycomptypeany:
			array_op_expr->useOr = true;
			break;

		case Edxlarraycomptypeall:
			array_op_expr->useOr = false;
			break;

		default:
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiPlStmt2DXLConversion,
				GPOS_WSZ_LIT(
					"Scalar Array Comparison: Specified operator type is invalid"));
	}

	// translate left and right child

	GPOS_ASSERT(2 == scalar_array_comp_node->Arity());

	CDXLNode *left_node = (*scalar_array_comp_node)[EdxlsccmpIndexLeft];
	CDXLNode *right_node = (*scalar_array_comp_node)[EdxlsccmpIndexRight];

	Expr *left_expr = TranslateDXLToScalar(left_node, colid_var);
	Expr *right_expr = TranslateDXLToScalar(right_node, colid_var);

	array_op_expr->args = ListMake2(left_expr, right_expr);
	// GPDB_91_MERGE_FIXME: collation
	array_op_expr->inputcollid =
		gpdb::ExprCollation((Node *) array_op_expr->args);

	return (Expr *) array_op_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarDistinctToScalar
//
//	@doc:
//		Translates a DXL scalar distinct comparison into a GPDB DistinctExpr node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarDistinctToScalar(
	const CDXLNode *scalar_distinct_cmp_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_distinct_cmp_node);
	CDXLScalarDistinctComp *dxlop =
		CDXLScalarDistinctComp::Cast(scalar_distinct_cmp_node->GetOperator());

	DistinctExpr *dist_expr = MakeNode(DistinctExpr);
	dist_expr->opno = CMDIdGPDB::CastMdid(dxlop->MDId())->Oid();

	const IMDScalarOp *md_scalar_op =
		m_md_accessor->RetrieveScOp(dxlop->MDId());

	dist_expr->opfuncid = CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();
	dist_expr->opresulttype =
		GetFunctionReturnTypeOid(md_scalar_op->FuncMdId());

	// translate left and right child
	GPOS_ASSERT(2 == scalar_distinct_cmp_node->Arity());
	CDXLNode *left_node = (*scalar_distinct_cmp_node)[EdxlscdistcmpIndexLeft];
	CDXLNode *right_node = (*scalar_distinct_cmp_node)[EdxlscdistcmpIndexRight];

	Expr *left_expr = TranslateDXLToScalar(left_node, colid_var);
	Expr *right_expr = TranslateDXLToScalar(right_node, colid_var);

	dist_expr->args = ListMake2(left_expr, right_expr);
	// GPDB_91_MERGE_FIXME: collation
	dist_expr->opcollid = gpdb::TypeCollation(dist_expr->opresulttype);
	dist_expr->inputcollid = gpdb::ExprCollation((Node *) dist_expr->args);

	return (Expr *) dist_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarAggrefToScalar
//
//	@doc:
//		Translates a DXL scalar aggref_node into a GPDB Aggref node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarAggrefToScalar(
	const CDXLNode *aggref_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != aggref_node);
	CDXLScalarAggref *dxlop =
		CDXLScalarAggref::Cast(aggref_node->GetOperator());

	Aggref *aggref = MakeNode(Aggref);
	aggref->aggfnoid = CMDIdGPDB::CastMdid(dxlop->GetDXLAggFuncMDid())->Oid();
	aggref->aggdistinct = NIL;
	aggref->agglevelsup = 0;
	aggref->aggkind = 'n';
	aggref->location = -1;
	aggref->aggtranstype = InvalidOid;
	aggref->aggargtypes = NIL;

	CMDIdGPDB *agg_mdid = GPOS_NEW(m_mp) CMDIdGPDB(aggref->aggfnoid);
	const IMDAggregate *pmdagg = m_md_accessor->RetrieveAgg(agg_mdid);
	agg_mdid->Release();

	EdxlAggrefStage edxlaggstage = dxlop->GetDXLAggStage();
	if (NULL != dxlop->GetDXLResolvedRetTypeMDid())
	{
		// use resolved type
		aggref->aggtype =
			CMDIdGPDB::CastMdid(dxlop->GetDXLResolvedRetTypeMDid())->Oid();
	}
	else if (EdxlaggstageIntermediate == edxlaggstage ||
			 EdxlaggstagePartial == edxlaggstage)
	{
		aggref->aggtype =
			CMDIdGPDB::CastMdid(pmdagg->GetIntermediateResultTypeMdid())->Oid();
	}
	else
	{
		aggref->aggtype =
			CMDIdGPDB::CastMdid(pmdagg->GetResultTypeMdid())->Oid();
	}

	switch (dxlop->GetDXLAggStage())
	{
		case EdxlaggstageNormal:
			aggref->aggsplit = AGGSPLIT_SIMPLE;
			break;
		case EdxlaggstagePartial:
			aggref->aggsplit = AGGSPLIT_INITIAL_SERIAL;
			break;
		case EdxlaggstageIntermediate:
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiPlStmt2DXLConversion,
				GPOS_WSZ_LIT(
					"GPDB_96_MERGE_FIXME: Intermediate aggregate stage not implemented"));
			break;
		case EdxlaggstageFinal:
			aggref->aggsplit = AGGSPLIT_FINAL_DESERIAL;
			break;
		default:
			GPOS_RAISE(
				gpdxl::ExmaDXL, gpdxl::ExmiPlStmt2DXLConversion,
				GPOS_WSZ_LIT("AGGREF: Specified AggStage value is invalid"));
	}

	// translate each DXL argument
	List *exprs = TranslateScalarChildren(aggref->args, aggref_node, colid_var);

	int attno;
	aggref->args = NIL;
	ListCell *lc;
	int sortgrpindex = 1;
	ForEachWithCount(lc, exprs, attno)
	{
		TargetEntry *new_target_entry =
			gpdb::MakeTargetEntry((Expr *) lfirst(lc), attno + 1, NULL, false);
		Oid aggargtype = gpdb::ExprType((Node *) lfirst(lc));
		/*
		 * Translate the aggdistinct bool set to true (in ORCA),
		 * to a List of SortGroupClause in the PLNSTMT
		 */
		if (dxlop->IsDistinct())
		{
			new_target_entry->ressortgroupref = sortgrpindex;
			SortGroupClause *gc = makeNode(SortGroupClause);
			gc->tleSortGroupRef = sortgrpindex;
			gc->eqop = gpdb::GetEqualityOp(
				gpdb::ExprType((Node *) new_target_entry->expr));
			gc->sortop = gpdb::GetOrderingOpForEqualityOp(gc->eqop, NULL);
			/*
			 * Since ORCA doesn't yet support ordered aggregates, we are
			 * setting nulls_first to false. This is also the default behavior
			 * when no order by clause is provided so it is OK to set it to
			 * false.
			 */
			gc->nulls_first = false;
			aggref->aggdistinct = gpdb::LAppend(aggref->aggdistinct, gc);
			sortgrpindex++;
		}
		aggref->args = gpdb::LAppend(aggref->args, new_target_entry);
		aggref->aggargtypes = gpdb::LAppendOid(aggref->aggargtypes, aggargtype);
	}

	/*
	 * Resolve the possibly-polymorphic aggregate transition type.
	 */
	Oid aggtranstype;
	Oid inputTypes[FUNC_MAX_ARGS];
	int numArguments;

	aggtranstype = gpdb::GetAggIntermediateResultType(aggref->aggfnoid);

	/* extract argument types (ignoring any ORDER BY expressions) */
	numArguments = gpdb::GetAggregateArgTypes(aggref, inputTypes);

	/* resolve actual type of transition state, if polymorphic */
	aggtranstype = gpdb::ResolveAggregateTransType(
		aggref->aggfnoid, aggtranstype, inputTypes, numArguments);
	aggref->aggtranstype = aggtranstype;

	// GPDB_91_MERGE_FIXME: collation
	aggref->inputcollid = gpdb::ExprCollation((Node *) exprs);
	aggref->aggcollid = gpdb::TypeCollation(aggref->aggtype);

	return (Expr *) aggref;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarWindowRefToScalar
//
//	@doc:
//		Translate a DXL scalar window ref into a GPDB WindowRef node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarWindowRefToScalar(
	const CDXLNode *scalar_winref_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_winref_node);
	CDXLScalarWindowRef *dxlop =
		CDXLScalarWindowRef::Cast(scalar_winref_node->GetOperator());

	WindowFunc *window_func = MakeNode(WindowFunc);
	window_func->winfnoid = CMDIdGPDB::CastMdid(dxlop->FuncMdId())->Oid();

	window_func->windistinct = dxlop->IsDistinct();
	window_func->location = -1;
	window_func->winref = dxlop->GetWindSpecPos() + 1;
	window_func->wintype = CMDIdGPDB::CastMdid(dxlop->ReturnTypeMdId())->Oid();
	window_func->winstar = dxlop->IsStarArg();
	window_func->winagg = dxlop->IsSimpleAgg();

	if (dxlop->GetDxlWinStage() != EdxlwinstageImmediate)
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiQuery2DXLError,
				   GPOS_WSZ_LIT("Unsupported window function stage"));

	// translate the arguments of the window function
	window_func->args = TranslateScalarChildren(window_func->args,
												scalar_winref_node, colid_var);
	// GPDB_91_MERGE_FIXME: collation
	window_func->wincollid = gpdb::TypeCollation(window_func->wintype);
	window_func->inputcollid = gpdb::ExprCollation((Node *) window_func->args);

	return (Expr *) window_func;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarFuncExprToScalar
//
//	@doc:
//		Translates a DXL scalar opexpr into a GPDB FuncExpr node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarFuncExprToScalar(
	const CDXLNode *scalar_func_expr_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_func_expr_node);
	CDXLScalarFuncExpr *dxlop =
		CDXLScalarFuncExpr::Cast(scalar_func_expr_node->GetOperator());

	FuncExpr *func_expr = MakeNode(FuncExpr);
	func_expr->funcid = CMDIdGPDB::CastMdid(dxlop->FuncMdId())->Oid();
	func_expr->funcretset = dxlop->ReturnsSet();
	func_expr->funcformat = COERCE_EXPLICIT_CALL;
	func_expr->funcresulttype =
		CMDIdGPDB::CastMdid(dxlop->ReturnTypeMdId())->Oid();
	func_expr->args = TranslateScalarChildren(func_expr->args,
											  scalar_func_expr_node, colid_var);

	// GPDB_91_MERGE_FIXME: collation
	func_expr->inputcollid = gpdb::ExprCollation((Node *) func_expr->args);
	func_expr->funccollid = gpdb::TypeCollation(func_expr->funcresulttype);

	return (Expr *) func_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarSubplanToScalar
//
//	@doc:
//		Translates a DXL scalar SubPlan into a GPDB SubPlan node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarSubplanToScalar(
	const CDXLNode *scalar_subplan_node, CMappingColIdVar *colid_var)
{
	CDXLTranslateContext *output_context =
		(dynamic_cast<CMappingColIdVarPlStmt *>(colid_var))->GetOutputContext();

	CContextDXLToPlStmt *dxl_to_plstmt_ctxt =
		(dynamic_cast<CMappingColIdVarPlStmt *>(colid_var))
			->GetDXLToPlStmtContext();

	CDXLScalarSubPlan *dxlop =
		CDXLScalarSubPlan::Cast(scalar_subplan_node->GetOperator());

	// translate subplan test expression
	List *param_ids = NIL;

	SubLinkType slink = CTranslatorUtils::MapDXLSubplanToSublinkType(
		dxlop->GetDxlSubplanType());
	Expr *test_expr = TranslateDXLSubplanTestExprToScalar(
		dxlop->GetDxlTestExpr(), slink, colid_var, &param_ids);

	const CDXLColRefArray *outer_refs = dxlop->GetDxlOuterColRefsArray();

	const ULONG len = outer_refs->Size();

	// Translate a copy of the translate context: the param mappings from the outer scope get copied in the constructor
	CDXLTranslateContext subplan_translate_ctxt(
		m_mp, output_context->IsParentAggNode(),
		output_context->GetColIdToParamIdMap());

	// insert new outer ref mappings in the subplan translate context
	for (ULONG ul = 0; ul < len; ul++)
	{
		CDXLColRef *dxl_colref = (*outer_refs)[ul];
		IMDId *mdid = dxl_colref->MdidType();
		ULONG colid = dxl_colref->Id();
		INT type_modifier = dxl_colref->TypeModifier();
		OID type_oid = CMDIdGPDB::CastMdid(mdid)->Oid();

		if (NULL == subplan_translate_ctxt.GetParamIdMappingElement(colid))
		{
			// keep outer reference mapping to the original column for subsequent subplans
			ULONG param_id = dxl_to_plstmt_ctxt->GetNextParamId(type_oid);
			CMappingElementColIdParamId *colid_to_param_id_map =
				GPOS_NEW(m_mp) CMappingElementColIdParamId(colid, param_id,
														   mdid, type_modifier);

			BOOL is_inserted GPOS_ASSERTS_ONLY =
				subplan_translate_ctxt.FInsertParamMapping(
					colid, colid_to_param_id_map);
			GPOS_ASSERT(is_inserted);
		}
	}

	CDXLNode *child_dxl = (*scalar_subplan_node)[0];
	GPOS_ASSERT(EdxloptypePhysical ==
				child_dxl->GetOperator()->GetDXLOperatorType());

	GPOS_ASSERT(NULL != scalar_subplan_node);
	GPOS_ASSERT(EdxlopScalarSubPlan ==
				scalar_subplan_node->GetOperator()->GetDXLOperator());
	GPOS_ASSERT(1 == scalar_subplan_node->Arity());

	// generate the child plan,
	// Translate DXL->PlStmt translator to handle subplan's relational children
	CTranslatorDXLToPlStmt dxl_to_plstmt_translator(
		m_mp, m_md_accessor,
		(dynamic_cast<CMappingColIdVarPlStmt *>(colid_var))
			->GetDXLToPlStmtContext(),
		m_num_of_segments);
	CDXLTranslationContextArray *prev_siblings_ctxt_arr =
		GPOS_NEW(m_mp) CDXLTranslationContextArray(m_mp);
	Plan *plan_child = dxl_to_plstmt_translator.TranslateDXLOperatorToPlan(
		child_dxl, &subplan_translate_ctxt, prev_siblings_ctxt_arr);
	prev_siblings_ctxt_arr->Release();

	GPOS_ASSERT(NULL != plan_child->targetlist &&
				1 <= gpdb::ListLength(plan_child->targetlist));

	// translate subplan and set test expression
	SubPlan *subplan =
		TranslateSubplanFromChildPlan(plan_child, slink, dxl_to_plstmt_ctxt);
	subplan->testexpr = (Node *) test_expr;
	subplan->paramIds = param_ids;

	// translate other subplan params
	TranslateSubplanParams(subplan, &subplan_translate_ctxt, outer_refs,
						   colid_var);

	return (Expr *) subplan;
}

inline BOOL
FDXLCastedId(CDXLNode *dxl_node)
{
	return EdxlopScalarCast == dxl_node->GetOperator()->GetDXLOperator() &&
		   dxl_node->Arity() > 0 &&
		   EdxlopScalarIdent == (*dxl_node)[0]->GetOperator()->GetDXLOperator();
}

inline CTranslatorDXLToScalar::STypeOidAndTypeModifier
OidParamOidFromDXLIdentOrDXLCastIdent(CDXLNode *ident_or_cast_ident_node)
{
	GPOS_ASSERT(EdxlopScalarIdent ==
					ident_or_cast_ident_node->GetOperator()->GetDXLOperator() ||
				FDXLCastedId(ident_or_cast_ident_node));

	CDXLScalarIdent *inner_ident;
	if (EdxlopScalarIdent ==
		ident_or_cast_ident_node->GetOperator()->GetDXLOperator())
	{
		inner_ident =
			CDXLScalarIdent::Cast(ident_or_cast_ident_node->GetOperator());
	}
	else
	{
		inner_ident = CDXLScalarIdent::Cast(
			(*ident_or_cast_ident_node)[0]->GetOperator());
	}
	Oid inner_type_oid = CMDIdGPDB::CastMdid(inner_ident->MdidType())->Oid();
	INT type_modifier = inner_ident->TypeModifier();

	CTranslatorDXLToScalar::STypeOidAndTypeModifier modifier = {inner_type_oid,
																type_modifier};
	return modifier;
}

//---------------------------------------------------------------------------
//      @function:
//              CTranslatorDXLToScalar::TranslateDXLSubplanTestExprToScalar
//
//      @doc:
//              Translate subplan test expression
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLSubplanTestExprToScalar(
	CDXLNode *test_expr_node, SubLinkType slink, CMappingColIdVar *colid_var,
	List **param_ids)
{
	if (EXPR_SUBLINK == slink || EXISTS_SUBLINK == slink ||
		NOT_EXISTS_SUBLINK == slink)
	{
		// expr/exists/not-exists sublinks have no test expression
		return NULL;
	}
	GPOS_ASSERT(NULL != test_expr_node);

	if (HasConstTrue(test_expr_node, m_md_accessor))
	{
		// dummy test expression
		return (Expr *) TranslateDXLScalarConstToScalar(test_expr_node, NULL);
	}

	if (EdxlopScalarCmp != test_expr_node->GetOperator()->GetDXLOperator())
	{
		// test expression is expected to be a comparison
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
				   GPOS_WSZ_LIT("Unexpected subplan test expression"));
	}

	GPOS_ASSERT(2 == test_expr_node->Arity());
	GPOS_ASSERT(ANY_SUBLINK == slink || ALL_SUBLINK == slink);

	CDXLNode *outer_child_node = (*test_expr_node)[0];
	CDXLNode *inner_child_node = (*test_expr_node)[1];

	if (EdxlopScalarIdent !=
			inner_child_node->GetOperator()->GetDXLOperator() &&
		!FDXLCastedId(inner_child_node))
	{
		// test expression is expected to be a comparison between an outer expression
		// and a scalar identifier from subplan child
		// ORCA currently only supports PARAMs on the inner side of the form id or cast(id)
		// The outer side may be any non-param thing.
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
				   GPOS_WSZ_LIT("Unsupported subplan test expression"));
	}

	// extract type of inner column
	CDXLScalarComp *scalar_cmp_dxl =
		CDXLScalarComp::Cast(test_expr_node->GetOperator());

	// create an OpExpr for subplan test expression
	OpExpr *op_expr = MakeNode(OpExpr);
	op_expr->opno = CMDIdGPDB::CastMdid(scalar_cmp_dxl->MDId())->Oid();
	const IMDScalarOp *md_scalar_op =
		m_md_accessor->RetrieveScOp(scalar_cmp_dxl->MDId());
	op_expr->opfuncid = CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();
	op_expr->opresulttype =
		CMDIdGPDB::CastMdid(m_md_accessor->PtMDType<IMDTypeBool>()->MDId())
			->Oid();
	op_expr->opretset = false;

	// translate outer expression (can be a deep scalar tree)
	Expr *outer_arg_expr = TranslateDXLToScalar(outer_child_node, colid_var);

	// add translated outer expression as first arg of OpExpr
	List *args = NIL;
	args = gpdb::LAppend(args, outer_arg_expr);

	// second arg must be an EXEC param which is replaced during query execution with subplan output
	Param *param = MakeNode(Param);
	param->paramkind = PARAM_EXEC;
	CContextDXLToPlStmt *dxl_to_plstmt_ctxt =
		(dynamic_cast<CMappingColIdVarPlStmt *>(colid_var))
			->GetDXLToPlStmtContext();
	CTranslatorDXLToScalar::STypeOidAndTypeModifier oidAndTypeModifier =
		OidParamOidFromDXLIdentOrDXLCastIdent(inner_child_node);
	param->paramid =
		dxl_to_plstmt_ctxt->GetNextParamId(oidAndTypeModifier.oid_type);
	param->paramtype = oidAndTypeModifier.oid_type;
	param->paramtypmod = oidAndTypeModifier.type_modifier;

	// test expression is used for non-scalar subplan,
	// second arg of test expression must be an EXEC param referring to subplan output,
	// we add this param to subplan param ids before translating other params

	*param_ids = gpdb::LAppendInt(*param_ids, param->paramid);

	if (EdxlopScalarIdent == inner_child_node->GetOperator()->GetDXLOperator())
	{
		args = gpdb::LAppend(args, param);
	}
	else  // we have a cast
	{
		CDXLScalarCast *scalar_cast =
			CDXLScalarCast::Cast(inner_child_node->GetOperator());
		Expr *pexprCastParam =
			TranslateRelabelTypeOrFuncExprFromDXL(scalar_cast, (Expr *) param);
		args = gpdb::LAppend(args, pexprCastParam);
	}
	op_expr->args = args;

	return (Expr *) op_expr;
}


//---------------------------------------------------------------------------
//      @function:
//              CTranslatorDXLToScalar::TranslateSubplanParams
//
//      @doc:
//              Translate subplan parameters
//
//---------------------------------------------------------------------------
void
CTranslatorDXLToScalar::TranslateSubplanParams(
	SubPlan *subplan, CDXLTranslateContext *dxl_translator_ctxt,
	const CDXLColRefArray *outer_refs, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != subplan);
	GPOS_ASSERT(NULL != dxl_translator_ctxt);
	GPOS_ASSERT(NULL != outer_refs);
	GPOS_ASSERT(NULL != colid_var);

	// Create the PARAM and ARG nodes
	const ULONG size = outer_refs->Size();
	for (ULONG ul = 0; ul < size; ul++)
	{
		CDXLColRef *dxl_colref = (*outer_refs)[ul];
		dxl_colref->AddRef();
		const CMappingElementColIdParamId *colid_to_param_id_map =
			dxl_translator_ctxt->GetParamIdMappingElement(dxl_colref->Id());

		// TODO: eliminate param, it's not *really* used, and it's (short-term) leaked
		Param *param = TranslateParamFromMapping(colid_to_param_id_map);
		subplan->parParam = gpdb::LAppendInt(subplan->parParam, param->paramid);

		GPOS_ASSERT(
			colid_to_param_id_map->MdidType()->Equals(dxl_colref->MdidType()));

		CDXLScalarIdent *scalar_ident_dxl =
			GPOS_NEW(m_mp) CDXLScalarIdent(m_mp, dxl_colref);
		Expr *arg = (Expr *) colid_var->VarFromDXLNodeScId(scalar_ident_dxl);

		// not found in mapping, it must be an external parameter
		if (NULL == arg)
		{
			arg = (Expr *) TranslateParamFromMapping(colid_to_param_id_map);
			GPOS_ASSERT(NULL != arg);
		}

		scalar_ident_dxl->Release();
		subplan->args = gpdb::LAppend(subplan->args, arg);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateSubplanFromChildPlan
//
//	@doc:
//		add child plan to translation context, and build a subplan from it
//
//---------------------------------------------------------------------------
SubPlan *
CTranslatorDXLToScalar::TranslateSubplanFromChildPlan(
	Plan *plan, SubLinkType slink, CContextDXLToPlStmt *dxl_to_plstmt_ctxt)
{
	dxl_to_plstmt_ctxt->AddSubplan(plan);

	SubPlan *subplan = MakeNode(SubPlan);
	subplan->plan_id =
		gpdb::ListLength(dxl_to_plstmt_ctxt->GetSubplanEntriesList());
	subplan->plan_name = GetSubplanAlias(subplan->plan_id);
	subplan->is_initplan = false;
	subplan->firstColType = gpdb::ExprType(
		(Node *) ((TargetEntry *) gpdb::ListNth(plan->targetlist, 0))->expr);
	// GPDB_91_MERGE_FIXME: collation
	subplan->firstColCollation = gpdb::TypeCollation(subplan->firstColType);
	subplan->firstColTypmod = -1;
	subplan->subLinkType = slink;
	subplan->is_multirow = false;
	subplan->unknownEqFalse = false;

	return subplan;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::GetSubplanAlias
//
//	@doc:
//		build plan name, for explain purposes
//
//---------------------------------------------------------------------------
CHAR *
CTranslatorDXLToScalar::GetSubplanAlias(ULONG plan_id)
{
	CWStringDynamic *plan_name = GPOS_NEW(m_mp) CWStringDynamic(m_mp);
	plan_name->AppendFormat(GPOS_WSZ_LIT("SubPlan %d"), plan_id);
	const WCHAR *buf = plan_name->GetBuffer();

	ULONG max_length = (GPOS_WSZ_LENGTH(buf) + 1) * GPOS_SIZEOF(WCHAR);
	CHAR *result_plan_name = (CHAR *) gpdb::GPDBAlloc(max_length);
	gpos::clib::Wcstombs(result_plan_name, const_cast<WCHAR *>(buf),
						 max_length);
	result_plan_name[max_length - 1] = '\0';
	GPOS_DELETE(plan_name);

	return result_plan_name;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateParamFromMapping
//
//	@doc:
//		Translates a GPDB param from the given mapping
//
//---------------------------------------------------------------------------
Param *
CTranslatorDXLToScalar::TranslateParamFromMapping(
	const CMappingElementColIdParamId *colid_to_param_id_map)
{
	Param *param = MakeNode(Param);
	param->paramid = colid_to_param_id_map->ParamId();
	param->paramkind = PARAM_EXEC;
	param->paramtype =
		CMDIdGPDB::CastMdid(colid_to_param_id_map->MdidType())->Oid();
	param->paramtypmod = colid_to_param_id_map->TypeModifier();
	// GPDB_91_MERGE_FIXME: collation
	param->paramcollid = gpdb::TypeCollation(param->paramtype);

	return param;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarBoolExprToScalar
//
//	@doc:
//		Translates a DXL scalar BoolExpr into a GPDB OpExpr node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarBoolExprToScalar(
	const CDXLNode *scalar_bool_expr_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_bool_expr_node);
	CDXLScalarBoolExpr *dxlop =
		CDXLScalarBoolExpr::Cast(scalar_bool_expr_node->GetOperator());
	BoolExpr *scalar_bool_expr = MakeNode(BoolExpr);

	GPOS_ASSERT(1 <= scalar_bool_expr_node->Arity());
	switch (dxlop->GetDxlBoolTypeStr())
	{
		case Edxlnot:
		{
			GPOS_ASSERT(1 == scalar_bool_expr_node->Arity());
			scalar_bool_expr->boolop = NOT_EXPR;
			break;
		}
		case Edxland:
		{
			GPOS_ASSERT(2 <= scalar_bool_expr_node->Arity());
			scalar_bool_expr->boolop = AND_EXPR;
			break;
		}
		case Edxlor:
		{
			GPOS_ASSERT(2 <= scalar_bool_expr_node->Arity());
			scalar_bool_expr->boolop = OR_EXPR;
			break;
		}
		default:
		{
			GPOS_ASSERT(!"Boolean Operation: Must be either or/ and / not");
			return NULL;
		}
	}

	scalar_bool_expr->args = TranslateScalarChildren(
		scalar_bool_expr->args, scalar_bool_expr_node, colid_var);
	scalar_bool_expr->location = -1;

	return (Expr *) scalar_bool_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarBooleanTestToScalar
//
//	@doc:
//		Translates a DXL scalar BooleanTest into a GPDB OpExpr node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarBooleanTestToScalar(
	const CDXLNode *scalar_boolean_test_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_boolean_test_node);
	CDXLScalarBooleanTest *dxlop =
		CDXLScalarBooleanTest::Cast(scalar_boolean_test_node->GetOperator());
	BooleanTest *scalar_boolean_test = MakeNode(BooleanTest);

	switch (dxlop->GetDxlBoolTypeStr())
	{
		case EdxlbooleantestIsTrue:
			scalar_boolean_test->booltesttype = IS_TRUE;
			break;
		case EdxlbooleantestIsNotTrue:
			scalar_boolean_test->booltesttype = IS_NOT_TRUE;
			break;
		case EdxlbooleantestIsFalse:
			scalar_boolean_test->booltesttype = IS_FALSE;
			break;
		case EdxlbooleantestIsNotFalse:
			scalar_boolean_test->booltesttype = IS_NOT_FALSE;
			break;
		case EdxlbooleantestIsUnknown:
			scalar_boolean_test->booltesttype = IS_UNKNOWN;
			break;
		case EdxlbooleantestIsNotUnknown:
			scalar_boolean_test->booltesttype = IS_NOT_UNKNOWN;
			break;
		default:
		{
			GPOS_ASSERT(!"Invalid Boolean Test Operation");
			return NULL;
		}
	}

	GPOS_ASSERT(1 == scalar_boolean_test_node->Arity());
	CDXLNode *dxlnode_arg = (*scalar_boolean_test_node)[0];

	Expr *arg_expr = TranslateDXLToScalar(dxlnode_arg, colid_var);
	scalar_boolean_test->arg = arg_expr;

	return (Expr *) scalar_boolean_test;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarNullTestToScalar
//
//	@doc:
//		Translates a DXL scalar NullTest into a GPDB NullTest node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarNullTestToScalar(
	const CDXLNode *scalar_null_test_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_null_test_node);
	CDXLScalarNullTest *dxlop =
		CDXLScalarNullTest::Cast(scalar_null_test_node->GetOperator());
	NullTest *null_test = MakeNode(NullTest);

	GPOS_ASSERT(1 == scalar_null_test_node->Arity());
	CDXLNode *child_dxl = (*scalar_null_test_node)[0];
	Expr *child_expr = TranslateDXLToScalar(child_dxl, colid_var);

	if (dxlop->IsNullTest())
	{
		null_test->nulltesttype = IS_NULL;
	}
	else
	{
		null_test->nulltesttype = IS_NOT_NULL;
	}

	null_test->arg = child_expr;
	return (Expr *) null_test;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarNullIfToScalar
//
//	@doc:
//		Translates a DXL scalar nullif into a GPDB NullIfExpr node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarNullIfToScalar(
	const CDXLNode *scalar_null_if_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_null_if_node);
	CDXLScalarNullIf *dxlop =
		CDXLScalarNullIf::Cast(scalar_null_if_node->GetOperator());

	NullIfExpr *scalar_null_if_expr = MakeNode(NullIfExpr);
	scalar_null_if_expr->opno = CMDIdGPDB::CastMdid(dxlop->MdIdOp())->Oid();

	const IMDScalarOp *md_scalar_op =
		m_md_accessor->RetrieveScOp(dxlop->MdIdOp());

	scalar_null_if_expr->opfuncid =
		CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();
	scalar_null_if_expr->opresulttype =
		CMDIdGPDB::CastMdid(dxlop->MdidType())->Oid();
	scalar_null_if_expr->opretset = false;

	// translate children
	GPOS_ASSERT(2 == scalar_null_if_node->Arity());
	scalar_null_if_expr->args = TranslateScalarChildren(
		scalar_null_if_expr->args, scalar_null_if_node, colid_var);
	// GPDB_91_MERGE_FIXME: collation
	scalar_null_if_expr->opcollid =
		gpdb::TypeCollation(scalar_null_if_expr->opresulttype);
	scalar_null_if_expr->inputcollid =
		gpdb::ExprCollation((Node *) scalar_null_if_expr->args);

	return (Expr *) scalar_null_if_expr;
}

Expr *
CTranslatorDXLToScalar::TranslateRelabelTypeOrFuncExprFromDXL(
	const CDXLScalarCast *scalar_cast, Expr *child_expr)
{
	if (IMDId::IsValid(scalar_cast->FuncMdId()))
	{
		FuncExpr *func_expr = MakeNode(FuncExpr);
		func_expr->funcid = CMDIdGPDB::CastMdid(scalar_cast->FuncMdId())->Oid();

		const IMDFunction *pmdfunc =
			m_md_accessor->RetrieveFunc(scalar_cast->FuncMdId());
		func_expr->funcretset = pmdfunc->ReturnsSet();
		;

		func_expr->funcformat = COERCE_IMPLICIT_CAST;
		func_expr->funcresulttype =
			CMDIdGPDB::CastMdid(scalar_cast->MdidType())->Oid();

		func_expr->args = NIL;
		func_expr->args = gpdb::LAppend(func_expr->args, child_expr);

		// GPDB_91_MERGE_FIXME: collation
		func_expr->inputcollid = gpdb::ExprCollation((Node *) func_expr->args);
		func_expr->funccollid = gpdb::TypeCollation(func_expr->funcresulttype);

		return (Expr *) func_expr;
	}

	RelabelType *relabel_type = MakeNode(RelabelType);

	relabel_type->resulttype =
		CMDIdGPDB::CastMdid(scalar_cast->MdidType())->Oid();
	relabel_type->arg = child_expr;
	relabel_type->resulttypmod = -1;
	relabel_type->location = -1;
	relabel_type->relabelformat = COERCE_IMPLICIT_CAST;
	// GPDB_91_MERGE_FIXME: collation
	relabel_type->resultcollid = gpdb::TypeCollation(relabel_type->resulttype);

	return (Expr *) relabel_type;
}

// Translates a DXL scalar cast into a GPDB RelabelType / FuncExpr node
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarCastToScalar(
	const CDXLNode *scalar_cast_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_cast_node);
	const CDXLScalarCast *dxlop =
		CDXLScalarCast::Cast(scalar_cast_node->GetOperator());

	GPOS_ASSERT(1 == scalar_cast_node->Arity());
	CDXLNode *child_dxl = (*scalar_cast_node)[0];

	Expr *child_expr = TranslateDXLToScalar(child_dxl, colid_var);

	return TranslateRelabelTypeOrFuncExprFromDXL(dxlop, child_expr);
}


//---------------------------------------------------------------------------
//      @function:
//              CTranslatorDXLToScalar::TranslateDXLScalarCoerceToDomainToScalar
//
//      @doc:
//              Translates a DXL scalar coerce into a GPDB coercetodomain node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarCoerceToDomainToScalar(
	const CDXLNode *scalar_coerce_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_coerce_node);
	CDXLScalarCoerceToDomain *dxlop =
		CDXLScalarCoerceToDomain::Cast(scalar_coerce_node->GetOperator());

	GPOS_ASSERT(1 == scalar_coerce_node->Arity());
	CDXLNode *child_dxl = (*scalar_coerce_node)[0];
	Expr *child_expr = TranslateDXLToScalar(child_dxl, colid_var);

	CoerceToDomain *coerce = MakeNode(CoerceToDomain);

	coerce->resulttype = CMDIdGPDB::CastMdid(dxlop->GetResultTypeMdId())->Oid();
	coerce->arg = child_expr;
	coerce->resulttypmod = dxlop->TypeModifier();
	coerce->location = dxlop->GetLocation();
	coerce->coercionformat = (CoercionForm) dxlop->GetDXLCoercionForm();
	// GPDB_91_MERGE_FIXME: collation
	coerce->resultcollid = gpdb::TypeCollation(coerce->resulttype);

	return (Expr *) coerce;
}

//---------------------------------------------------------------------------
//      @function:
//              CTranslatorDXLToScalar::TranslateDXLScalarCoerceViaIOToScalar
//
//      @doc:
//              Translates a DXL scalar coerce into a GPDB coerceviaio node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarCoerceViaIOToScalar(
	const CDXLNode *scalar_coerce_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_coerce_node);
	CDXLScalarCoerceViaIO *dxlop =
		CDXLScalarCoerceViaIO::Cast(scalar_coerce_node->GetOperator());

	GPOS_ASSERT(1 == scalar_coerce_node->Arity());
	CDXLNode *child_dxl = (*scalar_coerce_node)[0];
	Expr *child_expr = TranslateDXLToScalar(child_dxl, colid_var);

	CoerceViaIO *coerce = MakeNode(CoerceViaIO);

	coerce->resulttype = CMDIdGPDB::CastMdid(dxlop->GetResultTypeMdId())->Oid();
	coerce->arg = child_expr;
	coerce->location = dxlop->GetLocation();
	coerce->coerceformat = (CoercionForm) dxlop->GetDXLCoercionForm();
	// GPDB_91_MERGE_FIXME: collation
	coerce->resultcollid = gpdb::TypeCollation(coerce->resulttype);

	return (Expr *) coerce;
}

//---------------------------------------------------------------------------
//      @function:
//              CTranslatorDXLToScalar::TranslateDXLScalarArrayCoerceExprToScalar
//
//      @doc:
//              Translates a DXL scalar array coerce expr into a GPDB T_ArrayCoerceExpr node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarArrayCoerceExprToScalar(
	const CDXLNode *scalar_coerce_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_coerce_node);
	CDXLScalarArrayCoerceExpr *dxlop =
		CDXLScalarArrayCoerceExpr::Cast(scalar_coerce_node->GetOperator());

	GPOS_ASSERT(1 == scalar_coerce_node->Arity());
	CDXLNode *child_dxl = (*scalar_coerce_node)[0];

	Expr *child_expr = TranslateDXLToScalar(child_dxl, colid_var);

	ArrayCoerceExpr *coerce = MakeNode(ArrayCoerceExpr);

	coerce->arg = child_expr;
	Oid elemfuncid = CMDIdGPDB::CastMdid(dxlop->GetCoerceFuncMDid())->Oid();
	coerce->resulttype = CMDIdGPDB::CastMdid(dxlop->GetResultTypeMdId())->Oid();
	coerce->resulttypmod = dxlop->TypeModifier();
	// GPDB_91_MERGE_FIXME: collation
	coerce->resultcollid = gpdb::TypeCollation(coerce->resulttype);
	coerce->coerceformat = (CoercionForm) dxlop->GetDXLCoercionForm();
	coerce->location = dxlop->GetLocation();
	// GPDB_12_MERGE_FIXME: change the representation of
	// CDXLScalarArrayCoerceExpr so that we can correctly roundtrip
	CaseTestExpr *case_test_expr = MakeNode(CaseTestExpr);
	Oid input_array_type = gpdb::ExprType((Node *) child_expr);
	int32 input_array_elem_typmod = gpdb::ExprTypeMod((Node *) child_expr);
	case_test_expr->typeId = gpdb::GetElementType(input_array_type);
	case_test_expr->typeMod = input_array_elem_typmod;
	if (elemfuncid != 0)
	{
		FuncExpr *func_expr = MakeNode(FuncExpr);
		func_expr->funcid = elemfuncid;
		func_expr->funcformat = COERCE_EXPLICIT_CAST;
		// GPDB_12_MERGE_FIXME: shouldn't this come from the DXL as well?
		func_expr->funcresulttype = gpdb::GetFuncRetType(elemfuncid);
		// FIXME: this is a giant hack. We really should know the arity of the
		//   function we're calling. Instead, we're jamming three arguments,
		//   _always_
		func_expr->args = gpdb::LPrepend(
			case_test_expr, ListMake2(gpdb::MakeIntConst(dxlop->TypeModifier()),
									  gpdb::MakeBoolConst(true, false)));
		coerce->elemexpr = (Expr *) func_expr;
	}
	else
	{
		RelabelType *rt = MakeNode(RelabelType);
		rt->resulttypmod = dxlop->TypeModifier();
		rt->resulttype = gpdb::GetElementType(coerce->resulttype);
		rt->arg = (Expr *) case_test_expr;
		coerce->elemexpr = (Expr *) rt;
	}

	return (Expr *) coerce;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarCoalesceToScalar
//
//	@doc:
//		Translates a DXL scalar coalesce operator into a GPDB coalesce node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarCoalesceToScalar(
	const CDXLNode *scalar_coalesce_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_coalesce_node);
	CDXLScalarCoalesce *dxlop =
		CDXLScalarCoalesce::Cast(scalar_coalesce_node->GetOperator());
	CoalesceExpr *coalesce = MakeNode(CoalesceExpr);

	coalesce->coalescetype = CMDIdGPDB::CastMdid(dxlop->MdidType())->Oid();
	// GPDB_91_MERGE_FIXME: collation
	coalesce->coalescecollid = gpdb::TypeCollation(coalesce->coalescetype);
	coalesce->args = TranslateScalarChildren(coalesce->args,
											 scalar_coalesce_node, colid_var);
	coalesce->location = -1;

	return (Expr *) coalesce;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarMinMaxToScalar
//
//	@doc:
//		Translates a DXL scalar minmax operator into a GPDB minmax node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarMinMaxToScalar(
	const CDXLNode *scalar_min_max_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_min_max_node);
	CDXLScalarMinMax *dxlop =
		CDXLScalarMinMax::Cast(scalar_min_max_node->GetOperator());
	MinMaxExpr *min_max_expr = MakeNode(MinMaxExpr);

	min_max_expr->minmaxtype = CMDIdGPDB::CastMdid(dxlop->MdidType())->Oid();
	min_max_expr->minmaxcollid = gpdb::TypeCollation(min_max_expr->minmaxtype);
	min_max_expr->args = TranslateScalarChildren(
		min_max_expr->args, scalar_min_max_node, colid_var);
	// GPDB_91_MERGE_FIXME: collation
	min_max_expr->inputcollid =
		gpdb::ExprCollation((Node *) min_max_expr->args);
	min_max_expr->location = -1;

	CDXLScalarMinMax::EdxlMinMaxType min_max_type = dxlop->GetMinMaxType();
	if (CDXLScalarMinMax::EmmtMax == min_max_type)
	{
		min_max_expr->op = IS_GREATEST;
	}
	else
	{
		GPOS_ASSERT(CDXLScalarMinMax::EmmtMin == min_max_type);
		min_max_expr->op = IS_LEAST;
	}

	return (Expr *) min_max_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateScalarChildren
//
//	@doc:
//		Translate children of DXL node, and add them to list
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToScalar::TranslateScalarChildren(List *list,
												const CDXLNode *dxlnode,
												CMappingColIdVar *colid_var)
{
	List *new_list = list;

	const ULONG arity = dxlnode->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CDXLNode *child_dxl = (*dxlnode)[ul];
		Expr *child_expr = TranslateDXLToScalar(child_dxl, colid_var);
		new_list = gpdb::LAppend(new_list, child_expr);
	}

	return new_list;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarConstToScalar
//
//	@doc:
//		Translates a DXL scalar constant operator into a GPDB scalar const node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarConstToScalar(
	const CDXLNode *const_node,
	CMappingColIdVar *	//colid_var
)
{
	GPOS_ASSERT(NULL != const_node);
	CDXLScalarConstValue *dxlop =
		CDXLScalarConstValue::Cast(const_node->GetOperator());
	CDXLDatum *datum_dxl = const_cast<CDXLDatum *>(dxlop->GetDatumVal());

	return TranslateDXLDatumToScalar(datum_dxl);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLDatumToScalar
//
//	@doc:
//		Translates a DXL datum into a GPDB scalar const node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLDatumToScalar(CDXLDatum *datum_dxl)
{
	GPOS_ASSERT(NULL != datum_dxl);
	static const SDatumTranslatorElem translators[] = {
		{CDXLDatum::EdxldatumInt2,
		 &CTranslatorDXLToScalar::ConvertDXLDatumToConstInt2},
		{CDXLDatum::EdxldatumInt4,
		 &CTranslatorDXLToScalar::ConvertDXLDatumToConstInt4},
		{CDXLDatum::EdxldatumInt8,
		 &CTranslatorDXLToScalar::ConvertDXLDatumToConstInt8},
		{CDXLDatum::EdxldatumBool,
		 &CTranslatorDXLToScalar::ConvertDXLDatumToConstBool},
		{CDXLDatum::EdxldatumOid,
		 &CTranslatorDXLToScalar::ConvertDXLDatumToConstOid},
		{CDXLDatum::EdxldatumGeneric,
		 &CTranslatorDXLToScalar::TranslateDXLDatumGenericToScalar},
		{CDXLDatum::EdxldatumStatsDoubleMappable,
		 &CTranslatorDXLToScalar::TranslateDXLDatumGenericToScalar},
		{CDXLDatum::EdxldatumStatsLintMappable,
		 &CTranslatorDXLToScalar::TranslateDXLDatumGenericToScalar}};

	const ULONG num_translators = GPOS_ARRAY_SIZE(translators);
	CDXLDatum::EdxldatumType edxldatumtype = datum_dxl->GetDatumType();

	// find translator for the node type
	const_func_ptr translate_func = NULL;
	for (ULONG i = 0; i < num_translators; i++)
	{
		SDatumTranslatorElem elem = translators[i];
		if (edxldatumtype == elem.edxldt)
		{
			translate_func = elem.translate_func;
			break;
		}
	}

	if (NULL == translate_func)
	{
		GPOS_RAISE(
			gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtConversion,
			CDXLTokens::GetDXLTokenStr(EdxltokenScalarConstValue)->GetBuffer());
	}

	return (Expr *) (this->*translate_func)(datum_dxl);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::ConvertDXLDatumToConstOid
//
//	@doc:
//		Translates an oid datum into a constant
//
//---------------------------------------------------------------------------
Const *
CTranslatorDXLToScalar::ConvertDXLDatumToConstOid(CDXLDatum *datum_dxl)
{
	CDXLDatumOid *oid_datum_dxl = CDXLDatumOid::Cast(datum_dxl);

	Const *constant = MakeNode(Const);
	constant->consttype = CMDIdGPDB::CastMdid(oid_datum_dxl->MDId())->Oid();
	constant->consttypmod = -1;
	constant->constcollid = InvalidOid;
	constant->constbyval = true;
	constant->constisnull = oid_datum_dxl->IsNull();
	constant->constlen = sizeof(Oid);

	if (constant->constisnull)
	{
		constant->constvalue = (Datum) 0;
	}
	else
	{
		constant->constvalue = gpdb::DatumFromInt32(oid_datum_dxl->OidValue());
	}

	return constant;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::ConvertDXLDatumToConstInt2
//
//	@doc:
//		Translates an int2 datum into a constant
//
//---------------------------------------------------------------------------
Const *
CTranslatorDXLToScalar::ConvertDXLDatumToConstInt2(CDXLDatum *datum_dxl)
{
	CDXLDatumInt2 *datum_int2_dxl = CDXLDatumInt2::Cast(datum_dxl);

	Const *constant = MakeNode(Const);
	constant->consttype = CMDIdGPDB::CastMdid(datum_int2_dxl->MDId())->Oid();
	constant->consttypmod = -1;
	constant->constcollid = InvalidOid;
	constant->constbyval = true;
	constant->constisnull = datum_int2_dxl->IsNull();
	constant->constlen = sizeof(int16);

	if (constant->constisnull)
	{
		constant->constvalue = (Datum) 0;
	}
	else
	{
		constant->constvalue = gpdb::DatumFromInt16(datum_int2_dxl->Value());
	}

	return constant;
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::ConvertDXLDatumToConstInt4
//
//	@doc:
//		Translates an int4 datum into a constant
//
//---------------------------------------------------------------------------
Const *
CTranslatorDXLToScalar::ConvertDXLDatumToConstInt4(CDXLDatum *datum_dxl)
{
	CDXLDatumInt4 *datum_int4_dxl = CDXLDatumInt4::Cast(datum_dxl);

	Const *constant = MakeNode(Const);
	constant->consttype = CMDIdGPDB::CastMdid(datum_int4_dxl->MDId())->Oid();
	constant->consttypmod = -1;
	constant->constcollid = InvalidOid;
	constant->constbyval = true;
	constant->constisnull = datum_int4_dxl->IsNull();
	constant->constlen = sizeof(int32);

	if (constant->constisnull)
	{
		constant->constvalue = (Datum) 0;
	}
	else
	{
		constant->constvalue = gpdb::DatumFromInt32(datum_int4_dxl->Value());
	}

	return constant;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::ConvertDXLDatumToConstInt8
//
//	@doc:
//		Translates an int8 datum into a constant
//
//---------------------------------------------------------------------------
Const *
CTranslatorDXLToScalar::ConvertDXLDatumToConstInt8(CDXLDatum *datum_dxl)
{
	CDXLDatumInt8 *datum_int8_dxl = CDXLDatumInt8::Cast(datum_dxl);

	Const *constant = MakeNode(Const);
	constant->consttype = CMDIdGPDB::CastMdid(datum_int8_dxl->MDId())->Oid();
	constant->consttypmod = -1;
	constant->constcollid = InvalidOid;
	constant->constbyval = FLOAT8PASSBYVAL;
	constant->constisnull = datum_int8_dxl->IsNull();
	constant->constlen = sizeof(int64);

	if (constant->constisnull)
	{
		constant->constvalue = (Datum) 0;
	}
	else
	{
		constant->constvalue = gpdb::DatumFromInt64(datum_int8_dxl->Value());
	}

	return constant;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::ConvertDXLDatumToConstBool
//
//	@doc:
//		Translates a boolean datum into a constant
//
//---------------------------------------------------------------------------
Const *
CTranslatorDXLToScalar::ConvertDXLDatumToConstBool(CDXLDatum *datum_dxl)
{
	CDXLDatumBool *datum_bool_dxl = CDXLDatumBool::Cast(datum_dxl);

	Const *constant = MakeNode(Const);
	constant->consttype = CMDIdGPDB::CastMdid(datum_bool_dxl->MDId())->Oid();
	constant->consttypmod = -1;
	constant->constcollid = InvalidOid;
	constant->constbyval = true;
	constant->constisnull = datum_bool_dxl->IsNull();
	constant->constlen = sizeof(bool);

	if (constant->constisnull)
	{
		constant->constvalue = (Datum) 0;
	}
	else
	{
		constant->constvalue = gpdb::DatumFromBool(datum_bool_dxl->GetValue());
	}


	return constant;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLDatumGenericToScalar
//
//	@doc:
//		Translates a datum of generic type into a constant
//
//---------------------------------------------------------------------------
Const *
CTranslatorDXLToScalar::TranslateDXLDatumGenericToScalar(CDXLDatum *datum_dxl)
{
	CDXLDatumGeneric *datum_generic_dxl = CDXLDatumGeneric::Cast(datum_dxl);
	const IMDType *type =
		m_md_accessor->RetrieveType(datum_generic_dxl->MDId());

	Const *constant = MakeNode(Const);
	constant->consttype = CMDIdGPDB::CastMdid(datum_generic_dxl->MDId())->Oid();
	constant->consttypmod = datum_generic_dxl->TypeModifier();
	// GPDB_91_MERGE_FIXME: collation
	constant->constcollid = gpdb::TypeCollation(constant->consttype);
	constant->constbyval = type->IsPassedByValue();
	constant->constlen = type->Length();

	constant->constisnull = datum_generic_dxl->IsNull();
	if (constant->constisnull)
	{
		constant->constvalue = (Datum) 0;
	}
	else if (constant->constbyval)
	{
		// if it is a by-value constant, the value is stored in the datum.
		GPOS_ASSERT(constant->constlen >= 0);
		GPOS_ASSERT((ULONG) constant->constlen <= sizeof(Datum));
		memcpy(&constant->constvalue, datum_generic_dxl->GetByteArray(),
			   sizeof(Datum));
	}
	else
	{
		Datum val = gpdb::DatumFromPointer(datum_generic_dxl->GetByteArray());
		ULONG length = (ULONG) gpdb::DatumSize(val, false, constant->constlen);

		CHAR *str = (CHAR *) gpdb::GPDBAlloc(length + 1);
		memcpy(str, datum_generic_dxl->GetByteArray(), length);
		str[length] = '\0';
		constant->constvalue = gpdb::DatumFromPointer(str);
	}

	return constant;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarIdentToScalar
//
//	@doc:
//		Translates a DXL scalar ident into a GPDB Expr node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarIdentToScalar(
	const CDXLNode *scalar_id_node, CMappingColIdVar *colid_var)
{
	CMappingColIdVarPlStmt *colid_var_plstmt_map =
		dynamic_cast<CMappingColIdVarPlStmt *>(colid_var);

	// scalar identifier
	CDXLScalarIdent *dxlop =
		CDXLScalarIdent::Cast(scalar_id_node->GetOperator());
	Expr *result_expr = NULL;
	if (NULL == colid_var_plstmt_map ||
		NULL ==
			colid_var_plstmt_map->GetOutputContext()->GetParamIdMappingElement(
				dxlop->GetDXLColRef()->Id()))
	{
		// not an outer ref -> Translate var node
		result_expr = (Expr *) colid_var->VarFromDXLNodeScId(dxlop);
	}
	else
	{
		// outer ref -> Translate param node
		result_expr =
			(Expr *) colid_var_plstmt_map->ParamFromDXLNodeScId(dxlop);
	}

	if (NULL == result_expr)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXL2PlStmtAttributeNotFound,
				   dxlop->GetDXLColRef()->Id());
	}
	return result_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarCmpToScalar
//
//	@doc:
//		Translates a DXL scalar comparison operator or a scalar distinct comparison into a GPDB OpExpr node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarCmpToScalar(
	const CDXLNode *scalar_cmp_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_cmp_node);
	CDXLScalarComp *dxlop =
		CDXLScalarComp::Cast(scalar_cmp_node->GetOperator());

	OpExpr *op_expr = MakeNode(OpExpr);
	op_expr->opno = CMDIdGPDB::CastMdid(dxlop->MDId())->Oid();

	const IMDScalarOp *md_scalar_op =
		m_md_accessor->RetrieveScOp(dxlop->MDId());

	op_expr->opfuncid = CMDIdGPDB::CastMdid(md_scalar_op->FuncMdId())->Oid();
	op_expr->opresulttype =
		CMDIdGPDB::CastMdid(m_md_accessor->PtMDType<IMDTypeBool>()->MDId())
			->Oid();
	op_expr->opretset = false;

	// translate left and right child
	GPOS_ASSERT(2 == scalar_cmp_node->Arity());

	CDXLNode *left_node = (*scalar_cmp_node)[EdxlsccmpIndexLeft];
	CDXLNode *right_node = (*scalar_cmp_node)[EdxlsccmpIndexRight];

	Expr *left_expr = TranslateDXLToScalar(left_node, colid_var);
	Expr *right_expr = TranslateDXLToScalar(right_node, colid_var);

	op_expr->args = ListMake2(left_expr, right_expr);

	// GPDB_91_MERGE_FIXME: collation
	op_expr->inputcollid = gpdb::ExprCollation((Node *) op_expr->args);
	op_expr->opcollid = gpdb::TypeCollation(op_expr->opresulttype);
	;

	return (Expr *) op_expr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarArrayToScalar
//
//	@doc:
//		Translates a DXL scalar array into a GPDB ArrayExpr node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarArrayToScalar(
	const CDXLNode *scalar_array_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_array_node);
	CDXLScalarArray *dxlop =
		CDXLScalarArray::Cast(scalar_array_node->GetOperator());

	ArrayExpr *expr = MakeNode(ArrayExpr);
	expr->element_typeid = CMDIdGPDB::CastMdid(dxlop->ElementTypeMDid())->Oid();
	expr->array_typeid = CMDIdGPDB::CastMdid(dxlop->ArrayTypeMDid())->Oid();
	// GPDB_91_MERGE_FIXME: collation
	expr->array_collid = gpdb::TypeCollation(expr->array_typeid);
	expr->multidims = dxlop->IsMultiDimensional();
	expr->elements =
		TranslateScalarChildren(expr->elements, scalar_array_node, colid_var);

	/*
	 * ORCA doesn't know how to construct array constants, so it will
	 * return any arrays as ArrayExprs. Convert them to array constants,
	 * for more efficient evaluation at runtime. (This will try to further
	 * simplify the elements, too, but that is most likely futile, as the
	 * expressions were already simplified as much as we could before they
	 * were passed to ORCA. But there's little harm in trying).
	 */
	return (Expr *) gpdb::EvalConstExpressions((Node *) expr);
}

// GPDB_12_MERGE_FIXME: ArrayRef was renamed in commit 558d77f20e4e9.
// I've fixed the renamed type and fields but the wording "ArrayRef" is
// still everywhere. Do we plan to rename them?

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarArrayRefToScalar
//
//	@doc:
//		Translates a DXL scalar arrayref into a GPDB SubscriptingRef node
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarArrayRefToScalar(
	const CDXLNode *scalar_array_ref_node, CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != scalar_array_ref_node);
	CDXLScalarArrayRef *dxlop =
		CDXLScalarArrayRef::Cast(scalar_array_ref_node->GetOperator());

	SubscriptingRef *array_ref = MakeNode(SubscriptingRef);
	array_ref->refcontainertype =
		CMDIdGPDB::CastMdid(dxlop->ArrayTypeMDid())->Oid();
	array_ref->refelemtype =
		CMDIdGPDB::CastMdid(dxlop->ElementTypeMDid())->Oid();
	// GPDB_91_MERGE_FIXME: collation
	array_ref->refcollid = gpdb::TypeCollation(array_ref->refelemtype);
	array_ref->reftypmod = dxlop->TypeModifier();

	const ULONG arity = scalar_array_ref_node->Arity();
	GPOS_ASSERT(3 == arity || 4 == arity);

	array_ref->reflowerindexpr = TranslateDXLArrayRefIndexListToScalar(
		(*scalar_array_ref_node)[0], CDXLScalarArrayRefIndexList::EilbLower,
		colid_var);
	array_ref->refupperindexpr = TranslateDXLArrayRefIndexListToScalar(
		(*scalar_array_ref_node)[1], CDXLScalarArrayRefIndexList::EilbUpper,
		colid_var);

	array_ref->refexpr =
		TranslateDXLToScalar((*scalar_array_ref_node)[2], colid_var);
	array_ref->refassgnexpr = NULL;
	if (4 == arity)
	{
		array_ref->refassgnexpr =
			TranslateDXLToScalar((*scalar_array_ref_node)[3], colid_var);
	}

	return (Expr *) array_ref;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLArrayRefIndexListToScalar
//
//	@doc:
//		Translates a DXL arrayref index list
//
//---------------------------------------------------------------------------
List *
CTranslatorDXLToScalar::TranslateDXLArrayRefIndexListToScalar(
	const CDXLNode *index_list_node,
	CDXLScalarArrayRefIndexList::EIndexListBound
#ifdef GPOS_DEBUG
		index_list_bound
#endif	//GPOS_DEBUG
	,
	CMappingColIdVar *colid_var)
{
	GPOS_ASSERT(NULL != index_list_node);
	GPOS_ASSERT(index_list_bound == CDXLScalarArrayRefIndexList::Cast(
										index_list_node->GetOperator())
										->GetDXLIndexListBound());

	List *children = NIL;
	children = TranslateScalarChildren(children, index_list_node, colid_var);

	return children;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::TranslateDXLScalarDMLActionToScalar
//
//	@doc:
//		Translates a DML action expression
//
//---------------------------------------------------------------------------
Expr *
CTranslatorDXLToScalar::TranslateDXLScalarDMLActionToScalar(
	const CDXLNode *
#ifdef GPOS_DEBUG
		dml_action_node
#endif
	,
	CMappingColIdVar *	// colid_var
)
{
	GPOS_ASSERT(NULL != dml_action_node);
	GPOS_ASSERT(EdxlopScalarDMLAction ==
				dml_action_node->GetOperator()->GetDXLOperator());

	DMLActionExpr *expr = MakeNode(DMLActionExpr);

	return (Expr *) expr;
}



//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::GetFunctionReturnTypeOid
//
//	@doc:
//		Returns the operator return type oid for the operator funcid from the translation context
//
//---------------------------------------------------------------------------
Oid
CTranslatorDXLToScalar::GetFunctionReturnTypeOid(IMDId *mdid) const
{
	return CMDIdGPDB::CastMdid(
			   m_md_accessor->RetrieveFunc(mdid)->GetResultTypeMdid())
		->Oid();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::HasBoolResult
//
//	@doc:
//		Check to see if the operator returns a boolean result
//
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToScalar::HasBoolResult(CDXLNode *dxlnode,
									  CMDAccessor *md_accessor)
{
	GPOS_ASSERT(NULL != dxlnode);

	if (EdxloptypeScalar != dxlnode->GetOperator()->GetDXLOperatorType())
	{
		return false;
	}

	CDXLScalar *dxlop = dynamic_cast<CDXLScalar *>(dxlnode->GetOperator());

	return dxlop->HasBoolResult(md_accessor);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::HasConstTrue
//
//	@doc:
//		Check if the operator is a "true" bool constant
//
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToScalar::HasConstTrue(CDXLNode *dxlnode,
									 CMDAccessor *md_accessor)
{
	GPOS_ASSERT(NULL != dxlnode);
	if (!HasBoolResult(dxlnode, md_accessor) ||
		EdxlopScalarConstValue != dxlnode->GetOperator()->GetDXLOperator())
	{
		return false;
	}

	CDXLScalarConstValue *dxlop =
		CDXLScalarConstValue::Cast(dxlnode->GetOperator());
	CDXLDatumBool *datum_bool_dxl =
		CDXLDatumBool::Cast(const_cast<CDXLDatum *>(dxlop->GetDatumVal()));

	return datum_bool_dxl->GetValue();
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToScalar::HasConstNull
//
//	@doc:
//		Check if the operator is a NULL constant
//
//---------------------------------------------------------------------------
BOOL
CTranslatorDXLToScalar::HasConstNull(CDXLNode *dxlnode)
{
	GPOS_ASSERT(NULL != dxlnode);
	if (EdxlopScalarConstValue != dxlnode->GetOperator()->GetDXLOperator())
	{
		return false;
	}

	CDXLScalarConstValue *dxlop =
		CDXLScalarConstValue::Cast(dxlnode->GetOperator());

	return dxlop->GetDatumVal()->IsNull();
}

// EOF
