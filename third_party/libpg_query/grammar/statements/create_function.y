/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
 *****************************************************************************/
 CreateFunctionStmt:
			CREATE_P FUNCTION qualified_name '(' func_arg_list ')' AS a_expr
				{
					PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
					n->name = $3;
					n->args = $5;
					n->function = $8;
					$$ = (PGNode *)n;
				}
			| CREATE_P MACRO qualified_name '(' func_arg_list ')' AS a_expr
                        				{
                        					PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
                        					n->name = $3;
                        					n->args = $5;
                        					n->function = $8;
                        					$$ = (PGNode *)n;
                        				}
 		;
