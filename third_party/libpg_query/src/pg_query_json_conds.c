case T_Alias:
  _outAlias(str, obj);
  break;
case T_RangeVar:
  _outRangeVar(str, obj);
  break;
case T_Var:
  _outVar(str, obj);
  break;
case T_Param:
  _outParam(str, obj);
  break;
case T_Aggref:
  _outAggref(str, obj);
  break;
case T_GroupingFunc:
  _outGroupingFunc(str, obj);
  break;
case T_WindowFunc:
  _outWindowFunc(str, obj);
  break;
case T_ArrayRef:
  _outArrayRef(str, obj);
  break;
case T_FuncExpr:
  _outFuncExpr(str, obj);
  break;
case T_NamedArgExpr:
  _outNamedArgExpr(str, obj);
  break;
case T_OpExpr:
  _outOpExpr(str, obj);
  break;
case T_DistinctExpr:
  _outDistinctExpr(str, obj);
  break;
case T_NullIfExpr:
  _outNullIfExpr(str, obj);
  break;
case T_ScalarArrayOpExpr:
  _outScalarArrayOpExpr(str, obj);
  break;
case T_BoolExpr:
  _outBoolExpr(str, obj);
  break;
case T_SubLink:
  _outSubLink(str, obj);
  break;
case T_SubPlan:
  _outSubPlan(str, obj);
  break;
case T_AlternativeSubPlan:
  _outAlternativeSubPlan(str, obj);
  break;
case T_FieldSelect:
  _outFieldSelect(str, obj);
  break;
case T_FieldStore:
  _outFieldStore(str, obj);
  break;
case T_RelabelType:
  _outRelabelType(str, obj);
  break;
case T_CoerceViaIO:
  _outCoerceViaIO(str, obj);
  break;
case T_ArrayCoerceExpr:
  _outArrayCoerceExpr(str, obj);
  break;
case T_ConvertRowtypeExpr:
  _outConvertRowtypeExpr(str, obj);
  break;
case T_CollateExpr:
  _outCollateExpr(str, obj);
  break;
case T_CaseExpr:
  _outCaseExpr(str, obj);
  break;
case T_CaseWhen:
  _outCaseWhen(str, obj);
  break;
case T_CaseTestExpr:
  _outCaseTestExpr(str, obj);
  break;
case T_ArrayExpr:
  _outArrayExpr(str, obj);
  break;
case T_RowExpr:
  _outRowExpr(str, obj);
  break;
case T_RowCompareExpr:
  _outRowCompareExpr(str, obj);
  break;
case T_CoalesceExpr:
  _outCoalesceExpr(str, obj);
  break;
case T_MinMaxExpr:
  _outMinMaxExpr(str, obj);
  break;
case T_XmlExpr:
  _outXmlExpr(str, obj);
  break;
case T_NullTest:
  _outNullTest(str, obj);
  break;
case T_BooleanTest:
  _outBooleanTest(str, obj);
  break;
case T_CoerceToDomain:
  _outCoerceToDomain(str, obj);
  break;
case T_CoerceToDomainValue:
  _outCoerceToDomainValue(str, obj);
  break;
case T_SetToDefault:
  _outSetToDefault(str, obj);
  break;
case T_CurrentOfExpr:
  _outCurrentOfExpr(str, obj);
  break;
case T_InferenceElem:
  _outInferenceElem(str, obj);
  break;
case T_TargetEntry:
  _outTargetEntry(str, obj);
  break;
case T_RangeTblRef:
  _outRangeTblRef(str, obj);
  break;
case T_JoinExpr:
  _outJoinExpr(str, obj);
  break;
case T_FromExpr:
  _outFromExpr(str, obj);
  break;
case T_OnConflictExpr:
  _outOnConflictExpr(str, obj);
  break;
case T_IntoClause:
  _outIntoClause(str, obj);
  break;
case T_Query:
  _outQuery(str, obj);
  break;
case T_InsertStmt:
  _outInsertStmt(str, obj);
  break;
case T_DeleteStmt:
  _outDeleteStmt(str, obj);
  break;
case T_UpdateStmt:
  _outUpdateStmt(str, obj);
  break;
case T_SelectStmt:
  _outSelectStmt(str, obj);
  break;
case T_AlterTableStmt:
  _outAlterTableStmt(str, obj);
  break;
case T_AlterTableCmd:
  _outAlterTableCmd(str, obj);
  break;
case T_AlterDomainStmt:
  _outAlterDomainStmt(str, obj);
  break;
case T_SetOperationStmt:
  _outSetOperationStmt(str, obj);
  break;
case T_GrantStmt:
  _outGrantStmt(str, obj);
  break;
case T_GrantRoleStmt:
  _outGrantRoleStmt(str, obj);
  break;
case T_AlterDefaultPrivilegesStmt:
  _outAlterDefaultPrivilegesStmt(str, obj);
  break;
case T_ClosePortalStmt:
  _outClosePortalStmt(str, obj);
  break;
case T_ClusterStmt:
  _outClusterStmt(str, obj);
  break;
case T_CopyStmt:
  _outCopyStmt(str, obj);
  break;
case T_CreateStmt:
  _outCreateStmt(str, obj);
  break;
case T_DefineStmt:
  _outDefineStmt(str, obj);
  break;
case T_DropStmt:
  _outDropStmt(str, obj);
  break;
case T_TruncateStmt:
  _outTruncateStmt(str, obj);
  break;
case T_CommentStmt:
  _outCommentStmt(str, obj);
  break;
case T_FetchStmt:
  _outFetchStmt(str, obj);
  break;
case T_IndexStmt:
  _outIndexStmt(str, obj);
  break;
case T_CreateFunctionStmt:
  _outCreateFunctionStmt(str, obj);
  break;
case T_AlterFunctionStmt:
  _outAlterFunctionStmt(str, obj);
  break;
case T_DoStmt:
  _outDoStmt(str, obj);
  break;
case T_RenameStmt:
  _outRenameStmt(str, obj);
  break;
case T_RuleStmt:
  _outRuleStmt(str, obj);
  break;
case T_NotifyStmt:
  _outNotifyStmt(str, obj);
  break;
case T_ListenStmt:
  _outListenStmt(str, obj);
  break;
case T_UnlistenStmt:
  _outUnlistenStmt(str, obj);
  break;
case T_TransactionStmt:
  _outTransactionStmt(str, obj);
  break;
case T_ViewStmt:
  _outViewStmt(str, obj);
  break;
case T_LoadStmt:
  _outLoadStmt(str, obj);
  break;
case T_CreateDomainStmt:
  _outCreateDomainStmt(str, obj);
  break;
case T_CreatedbStmt:
  _outCreatedbStmt(str, obj);
  break;
case T_DropdbStmt:
  _outDropdbStmt(str, obj);
  break;
case T_VacuumStmt:
  _outVacuumStmt(str, obj);
  break;
case T_ExplainStmt:
  _outExplainStmt(str, obj);
  break;
case T_CreateTableAsStmt:
  _outCreateTableAsStmt(str, obj);
  break;
case T_CreateSeqStmt:
  _outCreateSeqStmt(str, obj);
  break;
case T_AlterSeqStmt:
  _outAlterSeqStmt(str, obj);
  break;
case T_VariableSetStmt:
  _outVariableSetStmt(str, obj);
  break;
case T_VariableShowStmt:
  _outVariableShowStmt(str, obj);
  break;
case T_DiscardStmt:
  _outDiscardStmt(str, obj);
  break;
case T_CreateTrigStmt:
  _outCreateTrigStmt(str, obj);
  break;
case T_CreatePLangStmt:
  _outCreatePLangStmt(str, obj);
  break;
case T_CreateRoleStmt:
  _outCreateRoleStmt(str, obj);
  break;
case T_AlterRoleStmt:
  _outAlterRoleStmt(str, obj);
  break;
case T_DropRoleStmt:
  _outDropRoleStmt(str, obj);
  break;
case T_LockStmt:
  _outLockStmt(str, obj);
  break;
case T_ConstraintsSetStmt:
  _outConstraintsSetStmt(str, obj);
  break;
case T_ReindexStmt:
  _outReindexStmt(str, obj);
  break;
case T_CheckPointStmt:
  _outCheckPointStmt(str, obj);
  break;
case T_CreateSchemaStmt:
  _outCreateSchemaStmt(str, obj);
  break;
case T_AlterDatabaseStmt:
  _outAlterDatabaseStmt(str, obj);
  break;
case T_AlterDatabaseSetStmt:
  _outAlterDatabaseSetStmt(str, obj);
  break;
case T_AlterRoleSetStmt:
  _outAlterRoleSetStmt(str, obj);
  break;
case T_CreateConversionStmt:
  _outCreateConversionStmt(str, obj);
  break;
case T_CreateCastStmt:
  _outCreateCastStmt(str, obj);
  break;
case T_CreateOpClassStmt:
  _outCreateOpClassStmt(str, obj);
  break;
case T_CreateOpFamilyStmt:
  _outCreateOpFamilyStmt(str, obj);
  break;
case T_AlterOpFamilyStmt:
  _outAlterOpFamilyStmt(str, obj);
  break;
case T_PrepareStmt:
  _outPrepareStmt(str, obj);
  break;
case T_ExecuteStmt:
  _outExecuteStmt(str, obj);
  break;
case T_DeallocateStmt:
  _outDeallocateStmt(str, obj);
  break;
case T_DeclareCursorStmt:
  _outDeclareCursorStmt(str, obj);
  break;
case T_CreateTableSpaceStmt:
  _outCreateTableSpaceStmt(str, obj);
  break;
case T_DropTableSpaceStmt:
  _outDropTableSpaceStmt(str, obj);
  break;
case T_AlterObjectSchemaStmt:
  _outAlterObjectSchemaStmt(str, obj);
  break;
case T_AlterOwnerStmt:
  _outAlterOwnerStmt(str, obj);
  break;
case T_DropOwnedStmt:
  _outDropOwnedStmt(str, obj);
  break;
case T_ReassignOwnedStmt:
  _outReassignOwnedStmt(str, obj);
  break;
case T_CompositeTypeStmt:
  _outCompositeTypeStmt(str, obj);
  break;
case T_CreateEnumStmt:
  _outCreateEnumStmt(str, obj);
  break;
case T_CreateRangeStmt:
  _outCreateRangeStmt(str, obj);
  break;
case T_AlterEnumStmt:
  _outAlterEnumStmt(str, obj);
  break;
case T_AlterTSDictionaryStmt:
  _outAlterTSDictionaryStmt(str, obj);
  break;
case T_AlterTSConfigurationStmt:
  _outAlterTSConfigurationStmt(str, obj);
  break;
case T_CreateFdwStmt:
  _outCreateFdwStmt(str, obj);
  break;
case T_AlterFdwStmt:
  _outAlterFdwStmt(str, obj);
  break;
case T_CreateForeignServerStmt:
  _outCreateForeignServerStmt(str, obj);
  break;
case T_AlterForeignServerStmt:
  _outAlterForeignServerStmt(str, obj);
  break;
case T_CreateUserMappingStmt:
  _outCreateUserMappingStmt(str, obj);
  break;
case T_AlterUserMappingStmt:
  _outAlterUserMappingStmt(str, obj);
  break;
case T_DropUserMappingStmt:
  _outDropUserMappingStmt(str, obj);
  break;
case T_AlterTableSpaceOptionsStmt:
  _outAlterTableSpaceOptionsStmt(str, obj);
  break;
case T_AlterTableMoveAllStmt:
  _outAlterTableMoveAllStmt(str, obj);
  break;
case T_SecLabelStmt:
  _outSecLabelStmt(str, obj);
  break;
case T_CreateForeignTableStmt:
  _outCreateForeignTableStmt(str, obj);
  break;
case T_ImportForeignSchemaStmt:
  _outImportForeignSchemaStmt(str, obj);
  break;
case T_CreateExtensionStmt:
  _outCreateExtensionStmt(str, obj);
  break;
case T_AlterExtensionStmt:
  _outAlterExtensionStmt(str, obj);
  break;
case T_AlterExtensionContentsStmt:
  _outAlterExtensionContentsStmt(str, obj);
  break;
case T_CreateEventTrigStmt:
  _outCreateEventTrigStmt(str, obj);
  break;
case T_AlterEventTrigStmt:
  _outAlterEventTrigStmt(str, obj);
  break;
case T_RefreshMatViewStmt:
  _outRefreshMatViewStmt(str, obj);
  break;
case T_ReplicaIdentityStmt:
  _outReplicaIdentityStmt(str, obj);
  break;
case T_AlterSystemStmt:
  _outAlterSystemStmt(str, obj);
  break;
case T_CreatePolicyStmt:
  _outCreatePolicyStmt(str, obj);
  break;
case T_AlterPolicyStmt:
  _outAlterPolicyStmt(str, obj);
  break;
case T_CreateTransformStmt:
  _outCreateTransformStmt(str, obj);
  break;
case T_A_Expr:
  _outA_Expr(str, obj);
  break;
case T_ColumnRef:
  _outColumnRef(str, obj);
  break;
case T_ParamRef:
  _outParamRef(str, obj);
  break;
case T_A_Const:
  _outA_Const(str, obj);
  break;
case T_FuncCall:
  _outFuncCall(str, obj);
  break;
case T_A_Star:
  _outA_Star(str, obj);
  break;
case T_A_Indices:
  _outA_Indices(str, obj);
  break;
case T_A_Indirection:
  _outA_Indirection(str, obj);
  break;
case T_A_ArrayExpr:
  _outA_ArrayExpr(str, obj);
  break;
case T_ResTarget:
  _outResTarget(str, obj);
  break;
case T_MultiAssignRef:
  _outMultiAssignRef(str, obj);
  break;
case T_TypeCast:
  _outTypeCast(str, obj);
  break;
case T_CollateClause:
  _outCollateClause(str, obj);
  break;
case T_SortBy:
  _outSortBy(str, obj);
  break;
case T_WindowDef:
  _outWindowDef(str, obj);
  break;
case T_RangeSubselect:
  _outRangeSubselect(str, obj);
  break;
case T_RangeFunction:
  _outRangeFunction(str, obj);
  break;
case T_RangeTableSample:
  _outRangeTableSample(str, obj);
  break;
case T_TypeName:
  _outTypeName(str, obj);
  break;
case T_ColumnDef:
  _outColumnDef(str, obj);
  break;
case T_IndexElem:
  _outIndexElem(str, obj);
  break;
case T_Constraint:
  _outConstraint(str, obj);
  break;
case T_DefElem:
  _outDefElem(str, obj);
  break;
case T_RangeTblEntry:
  _outRangeTblEntry(str, obj);
  break;
case T_RangeTblFunction:
  _outRangeTblFunction(str, obj);
  break;
case T_TableSampleClause:
  _outTableSampleClause(str, obj);
  break;
case T_WithCheckOption:
  _outWithCheckOption(str, obj);
  break;
case T_SortGroupClause:
  _outSortGroupClause(str, obj);
  break;
case T_GroupingSet:
  _outGroupingSet(str, obj);
  break;
case T_WindowClause:
  _outWindowClause(str, obj);
  break;
case T_FuncWithArgs:
  _outFuncWithArgs(str, obj);
  break;
case T_AccessPriv:
  _outAccessPriv(str, obj);
  break;
case T_CreateOpClassItem:
  _outCreateOpClassItem(str, obj);
  break;
case T_TableLikeClause:
  _outTableLikeClause(str, obj);
  break;
case T_FunctionParameter:
  _outFunctionParameter(str, obj);
  break;
case T_LockingClause:
  _outLockingClause(str, obj);
  break;
case T_RowMarkClause:
  _outRowMarkClause(str, obj);
  break;
case T_XmlSerialize:
  _outXmlSerialize(str, obj);
  break;
case T_WithClause:
  _outWithClause(str, obj);
  break;
case T_InferClause:
  _outInferClause(str, obj);
  break;
case T_OnConflictClause:
  _outOnConflictClause(str, obj);
  break;
case T_CommonTableExpr:
  _outCommonTableExpr(str, obj);
  break;
case T_RoleSpec:
  _outRoleSpec(str, obj);
  break;
case T_InlineCodeBlock:
  _outInlineCodeBlock(str, obj);
  break;
