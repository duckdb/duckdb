case T_Alias:
  _fingerprintAlias(ctx, obj, parent, field_name, depth);
  break;
case T_RangeVar:
  _fingerprintRangeVar(ctx, obj, parent, field_name, depth);
  break;
case T_Expr:
  _fingerprintExpr(ctx, obj, parent, field_name, depth);
  break;
case T_Var:
  _fingerprintVar(ctx, obj, parent, field_name, depth);
  break;
case T_Const:
  _fingerprintConst(ctx, obj, parent, field_name, depth);
  break;
case T_Param:
  _fingerprintParam(ctx, obj, parent, field_name, depth);
  break;
case T_Aggref:
  _fingerprintAggref(ctx, obj, parent, field_name, depth);
  break;
case T_GroupingFunc:
  _fingerprintGroupingFunc(ctx, obj, parent, field_name, depth);
  break;
case T_WindowFunc:
  _fingerprintWindowFunc(ctx, obj, parent, field_name, depth);
  break;
case T_ArrayRef:
  _fingerprintArrayRef(ctx, obj, parent, field_name, depth);
  break;
case T_FuncExpr:
  _fingerprintFuncExpr(ctx, obj, parent, field_name, depth);
  break;
case T_NamedArgExpr:
  _fingerprintNamedArgExpr(ctx, obj, parent, field_name, depth);
  break;
case T_OpExpr:
  _fingerprintOpExpr(ctx, obj, parent, field_name, depth);
  break;
case T_ScalarArrayOpExpr:
  _fingerprintScalarArrayOpExpr(ctx, obj, parent, field_name, depth);
  break;
case T_BoolExpr:
  _fingerprintBoolExpr(ctx, obj, parent, field_name, depth);
  break;
case T_SubLink:
  _fingerprintSubLink(ctx, obj, parent, field_name, depth);
  break;
case T_SubPlan:
  _fingerprintSubPlan(ctx, obj, parent, field_name, depth);
  break;
case T_AlternativeSubPlan:
  _fingerprintAlternativeSubPlan(ctx, obj, parent, field_name, depth);
  break;
case T_FieldSelect:
  _fingerprintFieldSelect(ctx, obj, parent, field_name, depth);
  break;
case T_FieldStore:
  _fingerprintFieldStore(ctx, obj, parent, field_name, depth);
  break;
case T_RelabelType:
  _fingerprintRelabelType(ctx, obj, parent, field_name, depth);
  break;
case T_CoerceViaIO:
  _fingerprintCoerceViaIO(ctx, obj, parent, field_name, depth);
  break;
case T_ArrayCoerceExpr:
  _fingerprintArrayCoerceExpr(ctx, obj, parent, field_name, depth);
  break;
case T_ConvertRowtypeExpr:
  _fingerprintConvertRowtypeExpr(ctx, obj, parent, field_name, depth);
  break;
case T_CollateExpr:
  _fingerprintCollateExpr(ctx, obj, parent, field_name, depth);
  break;
case T_CaseExpr:
  _fingerprintCaseExpr(ctx, obj, parent, field_name, depth);
  break;
case T_CaseWhen:
  _fingerprintCaseWhen(ctx, obj, parent, field_name, depth);
  break;
case T_CaseTestExpr:
  _fingerprintCaseTestExpr(ctx, obj, parent, field_name, depth);
  break;
case T_ArrayExpr:
  _fingerprintArrayExpr(ctx, obj, parent, field_name, depth);
  break;
case T_RowExpr:
  _fingerprintRowExpr(ctx, obj, parent, field_name, depth);
  break;
case T_RowCompareExpr:
  _fingerprintRowCompareExpr(ctx, obj, parent, field_name, depth);
  break;
case T_CoalesceExpr:
  _fingerprintCoalesceExpr(ctx, obj, parent, field_name, depth);
  break;
case T_MinMaxExpr:
  _fingerprintMinMaxExpr(ctx, obj, parent, field_name, depth);
  break;
case T_XmlExpr:
  _fingerprintXmlExpr(ctx, obj, parent, field_name, depth);
  break;
case T_NullTest:
  _fingerprintNullTest(ctx, obj, parent, field_name, depth);
  break;
case T_BooleanTest:
  _fingerprintBooleanTest(ctx, obj, parent, field_name, depth);
  break;
case T_CoerceToDomain:
  _fingerprintCoerceToDomain(ctx, obj, parent, field_name, depth);
  break;
case T_CoerceToDomainValue:
  _fingerprintCoerceToDomainValue(ctx, obj, parent, field_name, depth);
  break;
case T_SetToDefault:
  _fingerprintSetToDefault(ctx, obj, parent, field_name, depth);
  break;
case T_CurrentOfExpr:
  _fingerprintCurrentOfExpr(ctx, obj, parent, field_name, depth);
  break;
case T_InferenceElem:
  _fingerprintInferenceElem(ctx, obj, parent, field_name, depth);
  break;
case T_TargetEntry:
  _fingerprintTargetEntry(ctx, obj, parent, field_name, depth);
  break;
case T_RangeTblRef:
  _fingerprintRangeTblRef(ctx, obj, parent, field_name, depth);
  break;
case T_JoinExpr:
  _fingerprintJoinExpr(ctx, obj, parent, field_name, depth);
  break;
case T_FromExpr:
  _fingerprintFromExpr(ctx, obj, parent, field_name, depth);
  break;
case T_OnConflictExpr:
  _fingerprintOnConflictExpr(ctx, obj, parent, field_name, depth);
  break;
case T_IntoClause:
  _fingerprintIntoClause(ctx, obj, parent, field_name, depth);
  break;
case T_Query:
  _fingerprintQuery(ctx, obj, parent, field_name, depth);
  break;
case T_InsertStmt:
  _fingerprintInsertStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DeleteStmt:
  _fingerprintDeleteStmt(ctx, obj, parent, field_name, depth);
  break;
case T_UpdateStmt:
  _fingerprintUpdateStmt(ctx, obj, parent, field_name, depth);
  break;
case T_SelectStmt:
  _fingerprintSelectStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterTableStmt:
  _fingerprintAlterTableStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterTableCmd:
  _fingerprintAlterTableCmd(ctx, obj, parent, field_name, depth);
  break;
case T_AlterDomainStmt:
  _fingerprintAlterDomainStmt(ctx, obj, parent, field_name, depth);
  break;
case T_SetOperationStmt:
  _fingerprintSetOperationStmt(ctx, obj, parent, field_name, depth);
  break;
case T_GrantStmt:
  _fingerprintGrantStmt(ctx, obj, parent, field_name, depth);
  break;
case T_GrantRoleStmt:
  _fingerprintGrantRoleStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterDefaultPrivilegesStmt:
  _fingerprintAlterDefaultPrivilegesStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ClosePortalStmt:
  _fingerprintClosePortalStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ClusterStmt:
  _fingerprintClusterStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CopyStmt:
  _fingerprintCopyStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateStmt:
  _fingerprintCreateStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DefineStmt:
  _fingerprintDefineStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DropStmt:
  _fingerprintDropStmt(ctx, obj, parent, field_name, depth);
  break;
case T_TruncateStmt:
  _fingerprintTruncateStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CommentStmt:
  _fingerprintCommentStmt(ctx, obj, parent, field_name, depth);
  break;
case T_FetchStmt:
  _fingerprintFetchStmt(ctx, obj, parent, field_name, depth);
  break;
case T_IndexStmt:
  _fingerprintIndexStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateFunctionStmt:
  _fingerprintCreateFunctionStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterFunctionStmt:
  _fingerprintAlterFunctionStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DoStmt:
  _fingerprintDoStmt(ctx, obj, parent, field_name, depth);
  break;
case T_RenameStmt:
  _fingerprintRenameStmt(ctx, obj, parent, field_name, depth);
  break;
case T_RuleStmt:
  _fingerprintRuleStmt(ctx, obj, parent, field_name, depth);
  break;
case T_NotifyStmt:
  _fingerprintNotifyStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ListenStmt:
  _fingerprintListenStmt(ctx, obj, parent, field_name, depth);
  break;
case T_UnlistenStmt:
  _fingerprintUnlistenStmt(ctx, obj, parent, field_name, depth);
  break;
case T_TransactionStmt:
  _fingerprintTransactionStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ViewStmt:
  _fingerprintViewStmt(ctx, obj, parent, field_name, depth);
  break;
case T_LoadStmt:
  _fingerprintLoadStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateDomainStmt:
  _fingerprintCreateDomainStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreatedbStmt:
  _fingerprintCreatedbStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DropdbStmt:
  _fingerprintDropdbStmt(ctx, obj, parent, field_name, depth);
  break;
case T_VacuumStmt:
  _fingerprintVacuumStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ExplainStmt:
  _fingerprintExplainStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateTableAsStmt:
  _fingerprintCreateTableAsStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateSeqStmt:
  _fingerprintCreateSeqStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterSeqStmt:
  _fingerprintAlterSeqStmt(ctx, obj, parent, field_name, depth);
  break;
case T_VariableSetStmt:
  _fingerprintVariableSetStmt(ctx, obj, parent, field_name, depth);
  break;
case T_VariableShowStmt:
  _fingerprintVariableShowStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DiscardStmt:
  _fingerprintDiscardStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateTrigStmt:
  _fingerprintCreateTrigStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreatePLangStmt:
  _fingerprintCreatePLangStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateRoleStmt:
  _fingerprintCreateRoleStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterRoleStmt:
  _fingerprintAlterRoleStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DropRoleStmt:
  _fingerprintDropRoleStmt(ctx, obj, parent, field_name, depth);
  break;
case T_LockStmt:
  _fingerprintLockStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ConstraintsSetStmt:
  _fingerprintConstraintsSetStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ReindexStmt:
  _fingerprintReindexStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CheckPointStmt:
  _fingerprintCheckPointStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateSchemaStmt:
  _fingerprintCreateSchemaStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterDatabaseStmt:
  _fingerprintAlterDatabaseStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterDatabaseSetStmt:
  _fingerprintAlterDatabaseSetStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterRoleSetStmt:
  _fingerprintAlterRoleSetStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateConversionStmt:
  _fingerprintCreateConversionStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateCastStmt:
  _fingerprintCreateCastStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateOpClassStmt:
  _fingerprintCreateOpClassStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateOpFamilyStmt:
  _fingerprintCreateOpFamilyStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterOpFamilyStmt:
  _fingerprintAlterOpFamilyStmt(ctx, obj, parent, field_name, depth);
  break;
case T_PrepareStmt:
  _fingerprintPrepareStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ExecuteStmt:
  _fingerprintExecuteStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DeallocateStmt:
  _fingerprintDeallocateStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DeclareCursorStmt:
  _fingerprintDeclareCursorStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateTableSpaceStmt:
  _fingerprintCreateTableSpaceStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DropTableSpaceStmt:
  _fingerprintDropTableSpaceStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterObjectSchemaStmt:
  _fingerprintAlterObjectSchemaStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterOwnerStmt:
  _fingerprintAlterOwnerStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DropOwnedStmt:
  _fingerprintDropOwnedStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ReassignOwnedStmt:
  _fingerprintReassignOwnedStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CompositeTypeStmt:
  _fingerprintCompositeTypeStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateEnumStmt:
  _fingerprintCreateEnumStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateRangeStmt:
  _fingerprintCreateRangeStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterEnumStmt:
  _fingerprintAlterEnumStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterTSDictionaryStmt:
  _fingerprintAlterTSDictionaryStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterTSConfigurationStmt:
  _fingerprintAlterTSConfigurationStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateFdwStmt:
  _fingerprintCreateFdwStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterFdwStmt:
  _fingerprintAlterFdwStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateForeignServerStmt:
  _fingerprintCreateForeignServerStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterForeignServerStmt:
  _fingerprintAlterForeignServerStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateUserMappingStmt:
  _fingerprintCreateUserMappingStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterUserMappingStmt:
  _fingerprintAlterUserMappingStmt(ctx, obj, parent, field_name, depth);
  break;
case T_DropUserMappingStmt:
  _fingerprintDropUserMappingStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterTableSpaceOptionsStmt:
  _fingerprintAlterTableSpaceOptionsStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterTableMoveAllStmt:
  _fingerprintAlterTableMoveAllStmt(ctx, obj, parent, field_name, depth);
  break;
case T_SecLabelStmt:
  _fingerprintSecLabelStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateForeignTableStmt:
  _fingerprintCreateForeignTableStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ImportForeignSchemaStmt:
  _fingerprintImportForeignSchemaStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateExtensionStmt:
  _fingerprintCreateExtensionStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterExtensionStmt:
  _fingerprintAlterExtensionStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterExtensionContentsStmt:
  _fingerprintAlterExtensionContentsStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateEventTrigStmt:
  _fingerprintCreateEventTrigStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterEventTrigStmt:
  _fingerprintAlterEventTrigStmt(ctx, obj, parent, field_name, depth);
  break;
case T_RefreshMatViewStmt:
  _fingerprintRefreshMatViewStmt(ctx, obj, parent, field_name, depth);
  break;
case T_ReplicaIdentityStmt:
  _fingerprintReplicaIdentityStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterSystemStmt:
  _fingerprintAlterSystemStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreatePolicyStmt:
  _fingerprintCreatePolicyStmt(ctx, obj, parent, field_name, depth);
  break;
case T_AlterPolicyStmt:
  _fingerprintAlterPolicyStmt(ctx, obj, parent, field_name, depth);
  break;
case T_CreateTransformStmt:
  _fingerprintCreateTransformStmt(ctx, obj, parent, field_name, depth);
  break;
case T_A_Expr:
  _fingerprintA_Expr(ctx, obj, parent, field_name, depth);
  break;
case T_ColumnRef:
  _fingerprintColumnRef(ctx, obj, parent, field_name, depth);
  break;
case T_ParamRef:
  _fingerprintParamRef(ctx, obj, parent, field_name, depth);
  break;
case T_A_Const:
  _fingerprintA_Const(ctx, obj, parent, field_name, depth);
  break;
case T_FuncCall:
  _fingerprintFuncCall(ctx, obj, parent, field_name, depth);
  break;
case T_A_Star:
  _fingerprintA_Star(ctx, obj, parent, field_name, depth);
  break;
case T_A_Indices:
  _fingerprintA_Indices(ctx, obj, parent, field_name, depth);
  break;
case T_A_Indirection:
  _fingerprintA_Indirection(ctx, obj, parent, field_name, depth);
  break;
case T_A_ArrayExpr:
  _fingerprintA_ArrayExpr(ctx, obj, parent, field_name, depth);
  break;
case T_ResTarget:
  _fingerprintResTarget(ctx, obj, parent, field_name, depth);
  break;
case T_MultiAssignRef:
  _fingerprintMultiAssignRef(ctx, obj, parent, field_name, depth);
  break;
case T_TypeCast:
  _fingerprintTypeCast(ctx, obj, parent, field_name, depth);
  break;
case T_CollateClause:
  _fingerprintCollateClause(ctx, obj, parent, field_name, depth);
  break;
case T_SortBy:
  _fingerprintSortBy(ctx, obj, parent, field_name, depth);
  break;
case T_WindowDef:
  _fingerprintWindowDef(ctx, obj, parent, field_name, depth);
  break;
case T_RangeSubselect:
  _fingerprintRangeSubselect(ctx, obj, parent, field_name, depth);
  break;
case T_RangeFunction:
  _fingerprintRangeFunction(ctx, obj, parent, field_name, depth);
  break;
case T_RangeTableSample:
  _fingerprintRangeTableSample(ctx, obj, parent, field_name, depth);
  break;
case T_TypeName:
  _fingerprintTypeName(ctx, obj, parent, field_name, depth);
  break;
case T_ColumnDef:
  _fingerprintColumnDef(ctx, obj, parent, field_name, depth);
  break;
case T_IndexElem:
  _fingerprintIndexElem(ctx, obj, parent, field_name, depth);
  break;
case T_Constraint:
  _fingerprintConstraint(ctx, obj, parent, field_name, depth);
  break;
case T_DefElem:
  _fingerprintDefElem(ctx, obj, parent, field_name, depth);
  break;
case T_RangeTblEntry:
  _fingerprintRangeTblEntry(ctx, obj, parent, field_name, depth);
  break;
case T_RangeTblFunction:
  _fingerprintRangeTblFunction(ctx, obj, parent, field_name, depth);
  break;
case T_TableSampleClause:
  _fingerprintTableSampleClause(ctx, obj, parent, field_name, depth);
  break;
case T_WithCheckOption:
  _fingerprintWithCheckOption(ctx, obj, parent, field_name, depth);
  break;
case T_SortGroupClause:
  _fingerprintSortGroupClause(ctx, obj, parent, field_name, depth);
  break;
case T_GroupingSet:
  _fingerprintGroupingSet(ctx, obj, parent, field_name, depth);
  break;
case T_WindowClause:
  _fingerprintWindowClause(ctx, obj, parent, field_name, depth);
  break;
case T_FuncWithArgs:
  _fingerprintFuncWithArgs(ctx, obj, parent, field_name, depth);
  break;
case T_AccessPriv:
  _fingerprintAccessPriv(ctx, obj, parent, field_name, depth);
  break;
case T_CreateOpClassItem:
  _fingerprintCreateOpClassItem(ctx, obj, parent, field_name, depth);
  break;
case T_TableLikeClause:
  _fingerprintTableLikeClause(ctx, obj, parent, field_name, depth);
  break;
case T_FunctionParameter:
  _fingerprintFunctionParameter(ctx, obj, parent, field_name, depth);
  break;
case T_LockingClause:
  _fingerprintLockingClause(ctx, obj, parent, field_name, depth);
  break;
case T_RowMarkClause:
  _fingerprintRowMarkClause(ctx, obj, parent, field_name, depth);
  break;
case T_XmlSerialize:
  _fingerprintXmlSerialize(ctx, obj, parent, field_name, depth);
  break;
case T_WithClause:
  _fingerprintWithClause(ctx, obj, parent, field_name, depth);
  break;
case T_InferClause:
  _fingerprintInferClause(ctx, obj, parent, field_name, depth);
  break;
case T_OnConflictClause:
  _fingerprintOnConflictClause(ctx, obj, parent, field_name, depth);
  break;
case T_CommonTableExpr:
  _fingerprintCommonTableExpr(ctx, obj, parent, field_name, depth);
  break;
case T_RoleSpec:
  _fingerprintRoleSpec(ctx, obj, parent, field_name, depth);
  break;
case T_InlineCodeBlock:
  _fingerprintInlineCodeBlock(ctx, obj, parent, field_name, depth);
  break;
