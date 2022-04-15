/*****************************************************************************
 *
 * Create Type Statement
 *
 *****************************************************************************/

CreateTypeStmt:
                CREATE_P TYPE_P any_name opt_type_val_list
				{
                    PGCreateTypeStmt *n = makeNode(PGCreateTypeStmt);
					n->name = $3;
					n->vals = $4;
					$$ = (PGNode *)n;
				}
		;

opt_type_val_list:
		'(' ')'
			{
				$$ = NIL;
			}
		| '(' type_arg_list ')'
			{
				$$ = $2;
			}
	;

type_arg_list:	type_arg_expr
				{
					$$ = list_make1($1);
				}
			| type_arg_list ',' type_arg_expr
				{
					$$ = lappend($1, $3);
				}
		;

type_arg_expr:	param_name COLON_EQUALS Sconst
				{
					PGCustomTypeArgExpr *na = makeNode(PGCustomTypeArgExpr);
					na->name = $1;
					na->func_name = $3;
					$$ = (PGNode *) na;
				}
			;

