/*****************************************************************************
 *
 *		QUERY:
 *				INSERT STATEMENTS
 *
 *****************************************************************************/

InsertStmt:
			opt_with_clause INSERT opt_or_action INTO insert_target opt_by_name_or_position insert_rest
			opt_on_conflict returning_clause
				{
					$7->relation = $5;
					$7->onConflictAlias = $3;
					$7->onConflictClause = $8;
					$7->returningList = $9;
					$7->withClause = $1;
					$7->insert_column_order = $6;
					$$ = (PGNode *) $7;
				}
		;

insert_rest:
			SelectStmt
				{
					$$ = makeNode(PGInsertStmt);
					$$->cols = NIL;
					$$->selectStmt = $1;
				}
			| OVERRIDING override_kind VALUE_P SelectStmt
				{
					$$ = makeNode(PGInsertStmt);
					$$->cols = NIL;
					$$->override = $2;
					$$->selectStmt = $4;
				}
			| '(' insert_column_list ')' SelectStmt
				{
					$$ = makeNode(PGInsertStmt);
					$$->cols = $2;
					$$->selectStmt = $4;
				}
			| '(' insert_column_list ')' OVERRIDING override_kind VALUE_P SelectStmt
				{
					$$ = makeNode(PGInsertStmt);
					$$->cols = $2;
					$$->override = $5;
					$$->selectStmt = $7;
				}
			| DEFAULT VALUES
				{
					$$ = makeNode(PGInsertStmt);
					$$->cols = NIL;
					$$->selectStmt = NULL;
				}
		;


insert_target:
			qualified_name
				{
					$$ = $1;
				}
			| qualified_name AS ColId
				{
					$1->alias = makeAlias($3, NIL);
					$$ = $1;
				}
		;

opt_by_name_or_position:
		BY NAME_P				{ $$ = PG_INSERT_BY_NAME; }
		| BY POSITION			{ $$ = PG_INSERT_BY_POSITION; }
		| /* empty */			{ $$ = PG_INSERT_BY_POSITION; }
	;

opt_conf_expr:
			'(' index_params ')' where_clause
				{
					$$ = makeNode(PGInferClause);
					$$->indexElems = $2;
					$$->whereClause = $4;
					$$->conname = NULL;
					$$->location = @1;
				}
			|
			ON CONSTRAINT name
				{
					$$ = makeNode(PGInferClause);
					$$->indexElems = NIL;
					$$->whereClause = NULL;
					$$->conname = $3;
					$$->location = @1;
				}
			| /*EMPTY*/
				{
					$$ = NULL;
				}
		;


opt_with_clause:
		with_clause								{ $$ = $1; }
		| /*EMPTY*/								{ $$ = NULL; }
		;


insert_column_item:
			ColId opt_indirection
				{
					$$ = makeNode(PGResTarget);
					$$->name = $1;
					$$->indirection = check_indirection($2, yyscanner);
					$$->val = NULL;
					$$->location = @1;
				}
		;


set_clause:
			set_target '=' a_expr
				{
					$1->val = (PGNode *) $3;
					$$ = list_make1($1);
				}
			| '(' set_target_list ')' '=' a_expr
				{
					int ncolumns = list_length($2);
					int i = 1;
					PGListCell *col_cell;

					/* Create a PGMultiAssignRef source for each target */
					foreach(col_cell, $2)
					{
						PGResTarget *res_col = (PGResTarget *) lfirst(col_cell);
						PGMultiAssignRef *r = makeNode(PGMultiAssignRef);

						r->source = (PGNode *) $5;
						r->colno = i;
						r->ncolumns = ncolumns;
						res_col->val = (PGNode *) r;
						i++;
					}

					$$ = $2;
				}
		;


opt_or_action:
			OR REPLACE
				{
					$$ = PG_ONCONFLICT_ALIAS_REPLACE;
				}
			|
			OR IGNORE_P
				{
					$$ = PG_ONCONFLICT_ALIAS_IGNORE;
				}
			| /*EMPTY*/
				{
					$$ = PG_ONCONFLICT_ALIAS_NONE;
				}
			;

opt_on_conflict:
			ON CONFLICT opt_conf_expr DO UPDATE SET set_clause_list_opt_comma where_clause
				{
					$$ = makeNode(PGOnConflictClause);
					$$->action = PG_ONCONFLICT_UPDATE;
					$$->infer = $3;
					$$->targetList = $7;
					$$->whereClause = $8;
					$$->location = @1;
				}
			|
			ON CONFLICT opt_conf_expr DO NOTHING
				{
					$$ = makeNode(PGOnConflictClause);
					$$->action = PG_ONCONFLICT_NOTHING;
					$$->infer = $3;
					$$->targetList = NIL;
					$$->whereClause = NULL;
					$$->location = @1;
				}
			| /*EMPTY*/
				{
					$$ = NULL;
				}
		;


index_elem:	ColId opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(PGIndexElem);
					$$->name = $1;
					$$->expr = NULL;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| func_expr_windowless opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(PGIndexElem);
					$$->name = NULL;
					$$->expr = $1;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = $4;
					$$->nulls_ordering = $5;
				}
			| '(' a_expr ')' opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(PGIndexElem);
					$$->name = NULL;
					$$->expr = $2;
					$$->indexcolname = NULL;
					$$->collation = $4;
					$$->opclass = $5;
					$$->ordering = $6;
					$$->nulls_ordering = $7;
				}
		;


returning_clause:
			RETURNING target_list		{ $$ = $2; }
			| /* EMPTY */				{ $$ = NIL; }
		;



override_kind:
			USER		{ $$ = PG_OVERRIDING_USER_VALUE; }
			| SYSTEM_P	{ $$ = OVERRIDING_SYSTEM_VALUE; }
		;


set_target_list:
			set_target								{ $$ = list_make1($1); }
			| set_target_list ',' set_target		{ $$ = lappend($1,$3); }
		;




opt_collate: COLLATE any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;


opt_class:	any_name								{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;


insert_column_list:
			insert_column_item
					{ $$ = list_make1($1); }
			| insert_column_list ',' insert_column_item
					{ $$ = lappend($1, $3); }
		;


set_clause_list:
			set_clause							{ $$ = $1; }
			| set_clause_list ',' set_clause	{ $$ = list_concat($1,$3); }
		;

set_clause_list_opt_comma:
			set_clause_list								{ $$ = $1; }
			| set_clause_list ','							{ $$ = $1; }
		;

index_params:	index_elem							{ $$ = list_make1($1); }
			| index_params ',' index_elem			{ $$ = lappend($1, $3); }
		;


set_target:
			ColId opt_indirection
				{
					$$ = makeNode(PGResTarget);
					$$->name = $1;
					$$->indirection = check_indirection($2, yyscanner);
					$$->val = NULL;	/* upper production sets this */
					$$->location = @1;
				}
		;
