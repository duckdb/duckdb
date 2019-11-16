/*****************************************************************************
 *
 *		QUERY:
 *				DELETE STATEMENTS
 *
 *****************************************************************************/
DeleteStmt: opt_with_clause DELETE_P FROM relation_expr_opt_alias
			using_clause where_or_current_clause returning_clause
				{
					DeleteStmt *n = makeNode(DeleteStmt);
					n->relation = $4;
					n->usingClause = $5;
					n->whereClause = $6;
					n->returningList = $7;
					n->withClause = $1;
					$$ = (Node *)n;
				}
		;


relation_expr_opt_alias: relation_expr					%prec UMINUS
				{
					$$ = $1;
				}
			| relation_expr ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $2;
					$1->alias = alias;
					$$ = $1;
				}
			| relation_expr AS ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $3;
					$1->alias = alias;
					$$ = $1;
				}
		;


where_or_current_clause:
			WHERE a_expr							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;



using_clause:
				USING from_list						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
