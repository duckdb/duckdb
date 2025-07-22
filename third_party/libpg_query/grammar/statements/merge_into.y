/*****************************************************************************
 *
 *		QUERY:
 *				MERGE INTO
 *
 *****************************************************************************/
MergeIntoStmt: opt_with_clause
			MERGE INTO relation_expr_opt_alias
			USING table_ref
			join_qual
			merge_match_list
			returning_clause
				{
					PGMergeIntoStmt *n = makeNode(PGMergeIntoStmt);
					n->targetTable = $4;
					n->source = $6;
					if ($5 != NULL && IsA($7, PGList))
						n->usingClause = (PGList *) $7; /* USING clause */
					else
						n->joinCondition = $7; /* ON clause */
					n->matchActions = $8;
					n->withClause = $1;
					n->returningList = $9;
					$$ = (PGNode *)n;
				}
		;

opt_and_clause:
	AND a_expr		{ $$ = $2; }
	| /* EMPTY */	{ $$ = NULL; }
	;

opt_insert_column_list:
	'(' insert_column_list ')'		{ $$ = $2; }
	| /* EMPTY */					{ $$ = NULL; }
	;

opt_star_expr:
	'*' | /* EMPTY */
	;

matched_clause_action:
	UPDATE SET set_clause_list_opt_comma
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_UPDATE;
			n->insert_column_order = PG_INSERT_BY_POSITION;
			n->updateTargets = $3;
			$$ = (PGNode *)n;
		}
	| UPDATE SET '*'
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_UPDATE;
			n->insert_column_order = PG_INSERT_BY_POSITION;
			$$ = (PGNode *)n;
		}
	| UPDATE opt_by_name_or_position
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_UPDATE;
			n->insert_column_order = $2;
			$$ = (PGNode *)n;
		}
	| DELETE_P
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_DELETE;
			$$ = (PGNode *)n;
		}
	| INSERT opt_insert_column_list VALUES '(' expr_list_opt_comma ')'
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_INSERT;
			n->insert_column_order = PG_INSERT_BY_POSITION;
			n->insertCols = $2;
			n->insertValues = $5;
			$$ = (PGNode *)n;
		}
	| INSERT opt_by_name_or_position opt_star_expr
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_INSERT;
			n->insert_column_order = $2;
			$$ = (PGNode *)n;
		}
	| INSERT DEFAULT VALUES
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_INSERT;
			n->insert_column_order = PG_INSERT_BY_POSITION;
			n->defaultValues = true;
			$$ = (PGNode *)n;
		}
	| DO NOTHING
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_DO_NOTHING;
			$$ = (PGNode *)n;
		}
	| ERROR_P opt_error_message
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_ERROR;
			n->errorMessage = $2;
			$$ = (PGNode *)n;
		}
	;

opt_error_message:
	a_expr					{ $$ = $1; }
	| /* EMPTY */			{ $$ = NULL; }
	;

matched_clause:
	WHEN MATCHED opt_and_clause THEN matched_clause_action
		{
			PGMatchAction *n = (PGMatchAction *) $5;
			n->when = MERGE_ACTION_WHEN_MATCHED;
			n->andClause = $3;
			$$ = (PGNode *)n;
		}
	;

opt_source_or_target:
	BY SOURCE_P				{ $$ = MERGE_ACTION_WHEN_NOT_MATCHED_BY_SOURCE; }
	| BY TARGET_P			{ $$ = MERGE_ACTION_WHEN_NOT_MATCHED_BY_TARGET; }
	| /* empty */			{ $$ = MERGE_ACTION_WHEN_NOT_MATCHED_BY_TARGET; }
	;

not_matched_clause:
	WHEN NOT MATCHED opt_source_or_target opt_and_clause THEN matched_clause_action
		{
			PGMatchAction *n = (PGMatchAction *) $7;
			n->when = $4;
			n->andClause = $5;
			$$ = (PGNode *)n;
		}
	;

matched_or_not_matched_clause:
	matched_clause | not_matched_clause
	;

merge_match_list:
	matched_or_not_matched_clause						{ $$ = list_make1($1); }
	| matched_or_not_matched_clause merge_match_list	{ $$ = list_concat(list_make1($1), $2); }
	;
