/*****************************************************************************
 *
 *		QUERY:
 *				MERGE INTO
 *
 *****************************************************************************/
MergeIntoStmt: opt_with_clause
			MERGE INTO relation_expr_opt_alias
			USING table_ref
			ON a_expr
			merge_match_list
				{
					PGMergeIntoStmt *n = makeNode(PGMergeIntoStmt);
					n->targetTable = $4;
					n->source = $6;
					n->joinCondition = $8;
					n->matchActions = $9;
					n->withClause = $1;
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

matched_clause_action:
	UPDATE SET set_clause_list_opt_comma
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_UPDATE;
			n->updateTargets = $3;
			$$ = (PGNode *)n;
		}
	| DELETE_P
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_DELETE;
			$$ = (PGNode *)n;
		}
	| DO NOTHING
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_DO_NOTHING;
			$$ = (PGNode *)n;
		}
	| ABORT_P
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_ABORT;
			$$ = (PGNode *)n;
		}
	;

not_matched_clause_action:
	INSERT opt_insert_column_list VALUES '(' expr_list_opt_comma ')'
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_INSERT;
			n->insertCols = $2;
			n->insertValues = $5;
			$$ = (PGNode *)n;
		}
	| DO NOTHING
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_DO_NOTHING;
			$$ = (PGNode *)n;
		}
	| ABORT_P
		{
			PGMatchAction *n = makeNode(PGMatchAction);
			n->actionType = MERGE_ACTION_TYPE_ABORT;
			$$ = (PGNode *)n;
		}
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

not_matched_clause:
	WHEN NOT MATCHED opt_and_clause THEN not_matched_clause_action
		{
			PGMatchAction *n = (PGMatchAction *) $6;
			n->when = MERGE_ACTION_WHEN_NOT_MATCHED;
			n->andClause = $4;
			$$ = (PGNode *)n;
		}

matched_or_not_matched_clause:
	matched_clause | not_matched_clause
	;

merge_match_list:
	matched_or_not_matched_clause						{ $$ = list_make1($1); }
	| matched_or_not_matched_clause merge_match_list	{ $$ = list_concat(list_make1($1), $2); }
	;
