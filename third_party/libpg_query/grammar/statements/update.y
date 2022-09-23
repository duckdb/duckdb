/*****************************************************************************
 *
 *		QUERY:
 *				PGUpdateStmt (UPDATE)
 *
 *****************************************************************************/
UpdateStmt: opt_with_clause UPDATE relation_expr_opt_alias
			SET set_clause_list_opt_comma
			from_clause
			where_or_current_clause
			returning_clause
				{
					PGUpdateStmt *n = makeNode(PGUpdateStmt);
					n->relation = $3;
					n->targetList = $5;
					n->fromClause = $6;
					n->whereClause = $7;
					n->returningList = $8;
					n->withClause = $1;
					$$ = (PGNode *)n;
				}
		;
