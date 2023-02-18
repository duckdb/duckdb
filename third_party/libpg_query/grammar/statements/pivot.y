/*****************************************************************************
 *
 *		QUERY:
 *				PIVOT
 *
 *****************************************************************************/
PivotStmt:
			PIVOT table_ref USING func_application ON COLUMNS '(' name_list_opt_comma ')'
				{
					PGPivotStmt *n = makeNode(PGPivotStmt);
					n->source = $2;
					n->aggr = $4;
					n->columns = $8;
					$$ = (PGNode *)n;
				}
		|	PIVOT table_ref USING func_application ON COLUMNS '(' name_list_opt_comma ')' ROWS '(' name_list_opt_comma ')'
				{
					$$ = NULL;
				}
		|	PIVOT table_ref USING func_application ON ROWS '(' name_list_opt_comma ')' COLUMNS '(' name_list_opt_comma ')'
				{
					$$ = NULL;
				}
		;

