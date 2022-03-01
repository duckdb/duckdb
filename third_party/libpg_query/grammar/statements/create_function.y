/*****************************************************************************
 *
 * CREATE FUNCTION stmt
 *
  *****************************************************************************/
 CreateFunctionStmt:
                /* the OptTemp is present but not used - to avoid conflicts with other CREATE_P stPGCreateFunctionStmtatements */ 
		//CREATE_P OptTemp TABLE MACRO qualified_name param_list AS SelectStmt
		CREATE_P OptTemp macro_alias qualified_name param_list AS TABLE SelectStmt
			{
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				n->relpersistence=$2;
				n->name = $4;
				n->params = $5;
				n->function = NULL;
				n->query = $8;
				$$ = (PGNode *)n;
				
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
		CREATE_P OptTemp macro_alias qualified_name param_list AS a_expr
                         {
				PGCreateFunctionStmt *n = makeNode(PGCreateFunctionStmt);
				n->relpersistence=$2;
				n->name = $4;
				n->params = $5;
				n->function = $7;
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
