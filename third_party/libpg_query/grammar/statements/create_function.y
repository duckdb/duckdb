/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
  *****************************************************************************/
 CreateFunctionStmt:
                /* the OptTemp is present but not used - to avoid conflicts with other CREATE_P statements */ 
		CREATE_P OptTemp TABLE macro_alias qualified_name param_list AS SelectStmt
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				n->name = $5;
				n->params = $6;
				n->function = NULL;
				n->query = $8;
				$$ = (PGNode *)n;
				
                                /* need to consume OptTemp token so bison can complete */
				int persistence=$2;
			}
 		|

        /*
 		CREATE_P  macro_alias SCALAR qualified_name param_list AS b_expr
                	{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				n->name = $4;
				n->params = $5;
				n->function = $7;
				n->query = NULL;
				$$ = (PGNode *)n;
                	}
                 |
          */

		CREATE_P macro_alias qualified_name param_list AS a_expr
                         {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				n->name = $3;
				n->params = $4;
				n->function = $6;
				n->query = NULL;
				$$ = (PGNode *)n;
                         }

 		;



macro_alias:
		FUNCTION
		| MACRO


param_list:
		'(' ')'
			{
				$$ = NIL;
			}
		| '(' func_arg_list ')'
			{
				$$ = $2;
			}
	;
