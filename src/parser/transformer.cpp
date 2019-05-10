#include "parser/transformer.hpp"

#include "parser/expression/list.hpp"
#include "parser/statement/list.hpp"

using namespace duckdb;
using namespace postgres;
using namespace std;

bool Transformer::TransformParseTree(List *tree, vector<unique_ptr<SQLStatement>> &statements) {
	for (auto entry = tree->head; entry != nullptr; entry = entry->next) {
		auto stmt = TransformStatement((Node *)entry->data.ptr_value);
		if (!stmt) {
			statements.clear();
			return false;
		}
		statements.push_back(move(stmt));
	}
	return true;
}

unique_ptr<SQLStatement> Transformer::TransformStatement(Node *stmt) {
	switch (stmt->type) {
	case T_RawStmt:
		return TransformStatement(((RawStmt *)stmt)->stmt);
	case T_SelectStmt:
		return TransformSelect(stmt);
	case T_CreateStmt:
		return TransformCreateTable(stmt);
	case T_CreateSchemaStmt:
		return TransformCreateSchema(stmt);
	case T_ViewStmt:
		return TransformCreateView(stmt);
	case T_CreateSeqStmt:
		return TransformCreateSequence(stmt);
	case T_DropStmt:
		return TransformDrop(stmt);
	case T_InsertStmt:
		return TransformInsert(stmt);
	case T_CopyStmt:
		return TransformCopy(stmt);
	case T_TransactionStmt:
		return TransformTransaction(stmt);
	case T_DeleteStmt:
		return TransformDelete(stmt);
	case T_UpdateStmt:
		return TransformUpdate(stmt);
	case T_IndexStmt:
		return TransformCreateIndex(stmt);
	case T_AlterTableStmt:
		return TransformAlter(stmt);
	case T_RenameStmt:
		return TransformRename(stmt);
	case T_PrepareStmt:
		return TransformPrepare(stmt);
	case T_ExecuteStmt:
		return TransformExecute(stmt);
	case T_DeallocateStmt:
		return TransformDeallocate(stmt);
	case T_CreateTableAsStmt:
		return TransformCreateTableAs(stmt);
	case T_ExplainStmt: {
		ExplainStmt *explain_stmt = reinterpret_cast<ExplainStmt *>(stmt);
		return make_unique<ExplainStatement>(TransformStatement(explain_stmt->query));
	}
	default:
		throw NotImplementedException(NodetypeToString(stmt->type));
		// case T_Invalid:
		// 	break;
		// case T_IndexInfo:
		// 	break;
		// case T_ExprContext:
		// 	break;
		// case T_ProjectionInfo:
		// 	break;
		// case T_JunkFilter:
		// 	break;
		// case T_ResultRelInfo:
		// 	break;
		// case T_EState:
		// 	break;
		// case T_TupleTableSlot:
		// 	break;
		// case T_Plan:
		// 	break;
		// case T_Result:
		// 	break;
		// case T_ModifyTable:
		// 	break;
		// case T_Append:
		// 	break;
		// case T_MergeAppend:
		// 	break;
		// case T_RecursiveUnion:
		// 	break;
		// case T_BitmapAnd:
		// 	break;
		// case T_BitmapOr:
		// 	break;
		// case T_Scan:
		// 	break;
		// case T_SeqScan:
		// 	break;
		// case T_SampleScan:
		// 	break;
		// case T_IndexScan:
		// 	break;
		// case T_IndexOnlyScan:
		// 	break;
		// case T_BitmapIndexScan:
		// 	break;
		// case T_BitmapHeapScan:
		// 	break;
		// case T_TidScan:
		// 	break;
		// case T_SubqueryScan:
		// 	break;
		// case T_FunctionScan:
		// 	break;
		// case T_ValuesScan:
		// 	break;
		// case T_CteScan:
		// 	break;
		// case T_WorkTableScan:
		// 	break;
		// case T_ForeignScan:
		// 	break;
		// case T_CustomScan:
		// 	break;
		// case T_Join:
		// 	break;
		// case T_NestLoop:
		// 	break;
		// case T_MergeJoin:
		// 	break;
		// case T_HashJoin:
		// 	break;
		// case T_Material:
		// 	break;
		// case T_Sort:
		// 	break;
		// case T_Group:
		// 	break;
		// case T_Agg:
		// 	break;
		// case T_WindowAgg:
		// 	break;
		// case T_Unique:
		// 	break;
		// case T_Hash:
		// 	break;
		// case T_SetOp:
		// 	break;
		// case T_LockRows:
		// 	break;
		// case T_Limit:
		// 	break;
		// case T_NestLoopParam:
		// 	break;
		// case T_PlanRowMark:
		// 	break;
		// case T_PlanInvalItem:
		// 	break;
		// case T_PlanState:
		// 	break;
		// case T_ResultState:
		// 	break;
		// case T_ModifyTableState:
		// 	break;
		// case T_AppendState:
		// 	break;
		// case T_MergeAppendState:
		// 	break;
		// case T_RecursiveUnionState:
		// 	break;
		// case T_BitmapAndState:
		// 	break;
		// case T_BitmapOrState:
		// 	break;
		// case T_ScanState:
		// 	break;
		// case T_SeqScanState:
		// 	break;
		// case T_SampleScanState:
		// 	break;
		// case T_IndexScanState:
		// 	break;
		// case T_IndexOnlyScanState:
		// 	break;
		// case T_BitmapIndexScanState:
		// 	break;
		// case T_BitmapHeapScanState:
		// 	break;
		// case T_TidScanState:
		// 	break;
		// case T_SubqueryScanState:
		// 	break;
		// case T_FunctionScanState:
		// 	break;
		// case T_ValuesScanState:
		// 	break;
		// case T_CteScanState:
		// 	break;
		// case T_WorkTableScanState:
		// 	break;
		// case T_ForeignScanState:
		// 	break;
		// case T_CustomScanState:
		// 	break;
		// case T_JoinState:
		// 	break;
		// case T_NestLoopState:
		// 	break;
		// case T_MergeJoinState:
		// 	break;
		// case T_HashJoinState:
		// 	break;
		// case T_MaterialState:
		// 	break;
		// case T_SortState:
		// 	break;
		// case T_GroupState:
		// 	break;
		// case T_AggState:
		// 	break;
		// case T_WindowAggState:
		// 	break;
		// case T_UniqueState:
		// 	break;
		// case T_HashState:
		// 	break;
		// case T_SetOpState:
		// 	break;
		// case T_LockRowsState:
		// 	break;
		// case T_LimitState:
		// 	break;
		// case T_Alias:
		// 	break;
		// case T_RangeVar:
		// 	break;
		// case T_Expr:
		// 	break;
		// case T_Var:
		// 	break;
		// case T_Const:
		// 	break;
		// case T_Param:
		// 	break;
		// case T_Aggref:
		// 	break;
		// case T_GroupingFunc:
		// 	break;
		// case T_WindowFunc:
		// 	break;
		// case T_ArrayRef:
		// 	break;
		// case T_FuncExpr:
		// 	break;
		// case T_NamedArgExpr:
		// 	break;
		// case T_OpExpr:
		// 	break;
		// case T_DistinctExpr:
		// 	break;
		// case T_NullIfExpr:
		// 	break;
		// case T_ScalarArrayOpExpr:
		// 	break;
		// case T_BoolExpr:
		// 	break;
		// case T_SubLink:
		// 	break;
		// case T_SubPlan:
		// 	break;
		// case T_AlternativeSubPlan:
		// 	break;
		// case T_FieldSelect:
		// 	break;
		// case T_FieldStore:
		// 	break;
		// case T_RelabelType:
		// 	break;
		// case T_CoerceViaIO:
		// 	break;
		// case T_ArrayCoerceExpr:
		// 	break;
		// case T_ConvertRowtypeExpr:
		// 	break;
		// case T_CollateExpr:
		// 	break;
		// case T_CaseExpr:
		// 	break;
		// case T_CaseWhen:
		// 	break;
		// case T_CaseTestExpr:
		// 	break;
		// case T_ArrayExpr:
		// 	break;
		// case T_RowExpr:
		// 	break;
		// case T_RowCompareExpr:
		// 	break;
		// case T_CoalesceExpr:
		// 	break;
		// case T_MinMaxExpr:
		// 	break;
		// case T_XmlExpr:
		// 	break;
		// case T_NullTest:
		// 	break;
		// case T_BooleanTest:
		// 	break;
		// case T_CoerceToDomain:
		// 	break;
		// case T_CoerceToDomainValue:
		// 	break;
		// case T_SetToDefault:
		// 	break;
		// case T_CurrentOfExpr:
		// 	break;
		// case T_InferenceElem:
		// 	break;
		// case T_TargetEntry:
		// 	break;
		// case T_RangeTblRef:
		// 	break;
		// case T_JoinExpr:
		// 	break;
		// case T_FromExpr:
		// 	break;
		// case T_OnConflictExpr:
		// 	break;
		// case T_IntoClause:
		// 	break;
		// case T_ExprState:
		// 	break;
		// case T_GenericExprState:
		// 	break;
		// case T_WholeRowVarExprState:
		// 	break;
		// case T_AggrefExprState:
		// 	break;
		// case T_GroupingFuncExprState:
		// 	break;
		// case T_WindowFuncExprState:
		// 	break;
		// case T_ArrayRefExprState:
		// 	break;
		// case T_FuncExprState:
		// 	break;
		// case T_ScalarArrayOpExprState:
		// 	break;
		// case T_BoolExprState:
		// 	break;
		// case T_SubPlanState:
		// 	break;
		// case T_AlternativeSubPlanState:
		// 	break;
		// case T_FieldSelectState:
		// 	break;
		// case T_FieldStoreState:
		// 	break;
		// case T_CoerceViaIOState:
		// 	break;
		// case T_ArrayCoerceExprState:
		// 	break;
		// case T_ConvertRowtypeExprState:
		// 	break;
		// case T_CaseExprState:
		// 	break;
		// case T_CaseWhenState:
		// 	break;
		// case T_ArrayExprState:
		// 	break;
		// case T_RowExprState:
		// 	break;
		// case T_RowCompareExprState:
		// 	break;
		// case T_CoalesceExprState:
		// 	break;
		// case T_MinMaxExprState:
		// 	break;
		// case T_XmlExprState:
		// 	break;
		// case T_NullTestState:
		// 	break;
		// case T_CoerceToDomainState:
		// 	break;
		// case T_DomainConstraintState:
		// 	break;
		// case T_PlannerInfo:
		// 	break;
		// case T_PlannerGlobal:
		// 	break;
		// case T_RelOptInfo:
		// 	break;
		// case T_IndexOptInfo:
		// 	break;
		// case T_ParamPathInfo:
		// 	break;
		// case T_Path:
		// 	break;
		// case T_IndexPath:
		// 	break;
		// case T_BitmapHeapPath:
		// 	break;
		// case T_BitmapAndPath:
		// 	break;
		// case T_BitmapOrPath:
		// 	break;
		// case T_NestPath:
		// 	break;
		// case T_MergePath:
		// 	break;
		// case T_HashPath:
		// 	break;
		// case T_TidPath:
		// 	break;
		// case T_ForeignPath:
		// 	break;
		// case T_CustomPath:
		// 	break;
		// case T_AppendPath:
		// 	break;
		// case T_MergeAppendPath:
		// 	break;
		// case T_ResultPath:
		// 	break;
		// case T_MaterialPath:
		// 	break;
		// case T_UniquePath:
		// 	break;
		// case T_EquivalenceClass:
		// 	break;
		// case T_EquivalenceMember:
		// 	break;
		// case T_PathKey:
		// 	break;
		// case T_RestrictInfo:
		// 	break;
		// case T_PlaceHolderVar:
		// 	break;
		// case T_SpecialJoinInfo:
		// 	break;
		// case T_AppendRelInfo:
		// 	break;
		// case T_PlaceHolderInfo:
		// 	break;
		// case T_MinMaxAggInfo:
		// 	break;
		// case T_PlannerParamItem:
		// 	break;
		// case T_MemoryContext:
		// 	break;
		// case T_AllocSetContext:
		// 	break;
		// case T_Value:
		// 	break;
		// case T_Integer:
		// 	break;
		// case T_Float:
		// 	break;
		// case T_String:
		// 	break;
		// case T_BitString:
		// 	break;
		// case T_Null:
		// 	break;
		// case T_List:
		// 	break;
		// case T_IntList:
		// 	break;
		// case T_OidList:
		// 	break;
		// case T_Query:
		// 	break;
		// case T_PlannedStmt:
		// 	break;
		// case T_AlterTableCmd:
		// 	break;
		// case T_AlterDomainStmt:
		// 	break;
		// case T_SetOperationStmt:
		// 	break;
		// case T_GrantStmt:
		// 	break;
		// case T_GrantRoleStmt:
		// 	break;
		// case T_AlterDefaultPrivilegesStmt:
		// 	break;
		// case T_ClosePortalStmt:
		// 	break;
		// case T_ClusterStmt:
		// 	break;
		// case T_DefineStmt:
		// 	break;
		// case T_TruncateStmt:
		// 	break;
		// case T_CommentStmt:
		// 	break;
		// case T_FetchStmt:
		// 	break;
		// case T_CreateFunctionStmt:
		// 	break;
		// case T_AlterFunctionStmt:
		// 	break;
		// case T_DoStmt:
		// 	break;
		// case T_RuleStmt:
		// 	break;
		// case T_NotifyStmt:
		// 	break;
		// case T_ListenStmt:
		// 	break;
		// case T_UnlistenStmt:
		// 	break;
		// case T_LoadStmt:
		// 	break;
		// case T_CreateDomainStmt:
		// 	break;
		// case T_CreatedbStmt:
		// 	break;
		// case T_DropdbStmt:
		// 	break;
		// case T_VacuumStmt:
		// 	break;
		// case T_CreateTableAsStmt:
		// 	break;
		// case T_CreateSeqStmt:
		// 	break;
		// case T_AlterSeqStmt:
		// 	break;
		// case T_VariableSetStmt:
		// 	break;
		// case T_VariableShowStmt:
		// 	break;
		// case T_DiscardStmt:
		// 	break;
		// case T_CreateTrigStmt:
		// 	break;
		// case T_CreatePLangStmt:
		// 	break;
		// case T_CreateRoleStmt:
		// 	break;
		// case T_AlterRoleStmt:
		// 	break;
		// case T_DropRoleStmt:
		// 	break;
		// case T_LockStmt:
		// 	break;
		// case T_ConstraintsSetStmt:
		// 	break;
		// case T_ReindexStmt:
		// 	break;
		// case T_CheckPointStmt:
		// 	break;
		// case T_AlterDatabaseStmt:
		// 	break;
		// case T_AlterDatabaseSetStmt:
		// 	break;
		// case T_AlterRoleSetStmt:
		// 	break;
		// case T_CreateConversionStmt:
		// 	break;
		// case T_CreateCastStmt:
		// 	break;
		// case T_CreateOpClassStmt:
		// 	break;
		// case T_CreateOpFamilyStmt:
		// 	break;
		// case T_AlterOpFamilyStmt:
		// 	break;
		// case T_DeclareCursorStmt:
		// 	break;
		// case T_CreateTableSpaceStmt:
		// 	break;
		// case T_DropTableSpaceStmt:
		// 	break;
		// case T_AlterObjectSchemaStmt:
		// 	break;
		// case T_AlterOwnerStmt:
		// 	break;
		// case T_DropOwnedStmt:
		// 	break;
		// case T_ReassignOwnedStmt:
		// 	break;
		// case T_CompositeTypeStmt:
		// 	break;
		// case T_CreateEnumStmt:
		// 	break;
		// case T_CreateRangeStmt:
		// 	break;
		// case T_AlterEnumStmt:
		// 	break;
		// case T_AlterTSDictionaryStmt:
		// 	break;
		// case T_AlterTSConfigurationStmt:
		// 	break;
		// case T_CreateFdwStmt:
		// 	break;
		// case T_AlterFdwStmt:
		// 	break;
		// case T_CreateForeignServerStmt:
		// 	break;
		// case T_AlterForeignServerStmt:
		// 	break;
		// case T_CreateUserMappingStmt:
		// 	break;
		// case T_AlterUserMappingStmt:
		// 	break;
		// case T_DropUserMappingStmt:
		// 	break;
		// case T_AlterTableSpaceOptionsStmt:
		// 	break;
		// case T_AlterTableMoveAllStmt:
		// 	break;
		// case T_SecLabelStmt:
		// 	break;
		// case T_CreateForeignTableStmt:
		// 	break;
		// case T_ImportForeignSchemaStmt:
		// 	break;
		// case T_CreateExtensionStmt:
		// 	break;
		// case T_AlterExtensionStmt:
		// 	break;
		// case T_AlterExtensionContentsStmt:
		// 	break;
		// case T_CreateEventTrigStmt:
		// 	break;
		// case T_AlterEventTrigStmt:
		// 	break;
		// case T_RefreshMatViewStmt:
		// 	break;
		// case T_ReplicaIdentityStmt:
		// 	break;
		// case T_AlterSystemStmt:
		// 	break;
		// case T_CreatePolicyStmt:
		// 	break;
		// case T_AlterPolicyStmt:
		// 	break;
		// case T_CreateTransformStmt:
		// 	break;
		// case T_A_Expr:
		// 	break;
		// case T_ColumnRef:
		// 	break;
		// case T_ParamRef:
		// 	break;
		// case T_A_Const:
		// 	break;
		// case T_FuncCall:
		// 	break;
		// case T_A_Star:
		// 	break;
		// case T_A_Indices:
		// 	break;
		// case T_A_Indirection:
		// 	break;
		// case T_A_ArrayExpr:
		// 	break;
		// case T_ResTarget:
		// 	break;
		// case T_MultiAssignRef:
		// 	break;
		// case T_TypeCast:
		// 	break;
		// case T_CollateClause:
		// 	break;
		// case T_SortBy:
		// 	break;
		// case T_WindowDef:
		// 	break;
		// case T_RangeSubselect:
		// 	break;
		// case T_RangeFunction:
		// 	break;
		// case T_RangeTableSample:
		// 	break;
		// case T_TypeName:
		// 	break;
		// case T_ColumnDef:
		// 	break;
		// case T_IndexElem:
		// 	break;
		// case T_Constraint:
		// 	break;
		// case T_DefElem:
		// 	break;
		// case T_RangeTblEntry:
		// 	break;
		// case T_RangeTblFunction:
		// 	break;
		// case T_TableSampleClause:
		// 	break;
		// case T_WithCheckOption:
		// 	break;
		// case T_SortGroupClause:
		// 	break;
		// case T_GroupingSet:
		// 	break;
		// case T_WindowClause:
		// 	break;
		// case T_FuncWithArgs:
		// 	break;
		// case T_AccessPriv:
		// 	break;
		// case T_CreateOpClassItem:
		// 	break;
		// case T_TableLikeClause:
		// 	break;
		// case T_FunctionParameter:
		// 	break;
		// case T_LockingClause:
		// 	break;
		// case T_RowMarkClause:
		// 	break;
		// case T_XmlSerialize:
		// 	break;
		// case T_WithClause:
		// 	break;
		// case T_InferClause:
		// 	break;
		// case T_OnConflictClause:
		// 	break;
		// case T_CommonTableExpr:
		// 	break;
		// case T_RoleSpec:
		// 	break;
		// case T_IdentifySystemCmd:
		// 	break;
		// case T_BaseBackupCmd:
		// 	break;
		// case T_CreateReplicationSlotCmd:
		// 	break;
		// case T_DropReplicationSlotCmd:
		// 	break;
		// case T_StartReplicationCmd:
		// 	break;
		// case T_TimeLineHistoryCmd:
		// 	break;
		// case T_TriggerData:
		// 	break;
		// case T_EventTriggerData:
		// 	break;
		// case T_ReturnSetInfo:
		// 	break;
		// case T_WindowObjectData:
		// 	break;
		// case T_TIDBitmap:
		// 	break;
		// case T_InlineCodeBlock:
		// 	break;
		// case T_FdwRoutine:
		// 	break;
		// case T_TsmRoutine:
		// 	break;
	}
	return nullptr;
}
