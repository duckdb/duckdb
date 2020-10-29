/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
 *****************************************************************************/
 CreateFunctionStmt:
			CREATE_P FUNCTION ColId '(' func_arg_list ')' AS a_expr
				{
					PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
					n->args = $4;
					n->function = $6;
					$$ = (PGNode *)n;
				}
 		;