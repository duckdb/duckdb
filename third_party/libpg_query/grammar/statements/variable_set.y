/*****************************************************************************
 *
 * Set PG internal variable
 *	  SET name TO 'var_value'
 * Include SQL syntax (thomas 1997-10-22):
 *	  SET TIME ZONE 'var_value'
 *
 *****************************************************************************/
VariableSetStmt:
			SET set_rest
				{
					PGVariableSetStmt *n = $2;
					n->is_local = false;
					$$ = (PGNode *) n;
				}
			| SET LOCAL set_rest
				{
					PGVariableSetStmt *n = $3;
					n->is_local = true;
					$$ = (PGNode *) n;
				}
			| SET SESSION set_rest
				{
					PGVariableSetStmt *n = $3;
					n->is_local = false;
					$$ = (PGNode *) n;
				}
		;


set_rest:	/* Generic SET syntaxes: */
			generic_set 						{$$ = $1;}
			| var_name FROM CURRENT_P
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_SET_CURRENT;
					n->name = $1;
					$$ = n;
				}
			/* Special syntaxes mandated by SQL standard: */
			| TIME ZONE zone_value
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = (char*) "timezone";
					if ($3 != NULL)
						n->args = list_make1($3);
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| SCHEMA Sconst
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = (char*) "search_path";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
		;


generic_set:
			var_name TO var_list
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name '=' var_list
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					$$ = n;
				}
			| var_name TO DEFAULT
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
			| var_name '=' DEFAULT
				{
					PGVariableSetStmt *n = makeNode(PGVariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
		;


var_value:	opt_boolean_or_string
				{ $$ = makeStringConst($1, @1); }
			| NumericOnly
				{ $$ = makeAConst($1, @1); }
		;


zone_value:
			Sconst
				{
					$$ = makeStringConst($1, @1);
				}
			| IDENT
				{
					$$ = makeStringConst($1, @1);
				}
			| ConstInterval Sconst opt_interval
				{
					PGTypeName *t = $1;
					if ($3 != NIL)
					{
						PGAConst *n = (PGAConst *) linitial($3);
						if ((n->val.val.ival & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(PG_ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 parser_errposition(@3)));
					}
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst
				{
					PGTypeName *t = $1;
					t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| NumericOnly							{ $$ = makeAConst($1, @1); }
			| DEFAULT								{ $$ = NULL; }
			| LOCAL									{ $$ = NULL; }
		;


var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
		;
