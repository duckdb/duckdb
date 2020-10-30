/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
 *****************************************************************************/
 CreateFunctionStmt:
			CREATE_P FUNCTION ColId '(' func_arg_list ')' AS a_expr
				{
					PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
					n->name = $3;
					n->args = $5;
					n->function = $8;
					$$ = (PGNode *)n;
				}
 		;