/*****************************************************************************
 *
 * Manipulate variables
 *
 *****************************************************************************/
stmt: VariableSetStmt VariableResetStmt VariableShowStmt
%type <vsetstmt> generic_set set_rest generic_reset reset_rest
%type <str> var_name
%type <list>	var_list
%type <node>	var_value zone_value

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
					throw ParserException("SET");
				}
			| SET LOCAL set_rest
				{
					throw ParserException("SET LOCAL");
				}
			| SET SESSION set_rest
				{
					throw ParserException("SET SESSION");
				}
		;

generic_set:
			var_name TO var_list
				{
					throw ParserException("SET VARIABLE");
				}
			| var_name '=' var_list
				{
					throw ParserException("SET VARIABLE");
				}
			| var_name TO DEFAULT
				{
					throw ParserException("SET VARIABLE");
				}
			| var_name '=' DEFAULT
				{
					throw ParserException("SET VARIABLE");
				}
		;

set_rest:	/* Generic SET syntaxes: */
			generic_set 						{$$ = $1;}
			/* Special syntaxes mandated by SQL standard: */
			| TIME ZONE zone_value
				{
					throw ParserException("SET TIME ZONE");
				}
			| SCHEMA Sconst
				{
					throw ParserException("SET SCHEMA");
				}
		;

var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{ $$ = psprintf("%s.%s", $1, $3); }
		;

var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
		;

var_value:	opt_boolean_or_string
				{ $$ = makeStringConst($1, @1); }
			| NumericOnly
				{ $$ = makeAConst($1, @1); }
		;

/* Timezone values can be:
 * - a string such as 'pst8pdt'
 * - an identifier such as "pst8pdt"
 * - an integer or floating point number
 * - a time interval per SQL99
 * ColId gives reduce/reduce errors against ConstInterval and LOCAL,
 * so use IDENT (meaning we reject anything that is a key word).
 */
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
					TypeName *t = $1;
					if ($3 != NIL)
					{
						A_Const *n = (A_Const *) linitial($3);
						if ((n->val.val.ival & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 parser_errposition(@3)));
					}
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst
				{
					TypeName *t = $1;
					t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
											makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| NumericOnly							{ $$ = makeAConst($1, @1); }
			| DEFAULT								{ $$ = NULL; }
			| LOCAL									{ $$ = NULL; }
		;

/*****************************************************************************
 *
 * RESET variable;
 *
 *****************************************************************************/
VariableResetStmt:
			RESET reset_rest						{ $$ = (Node *) $2; }
		;

reset_rest:
			generic_reset							{ $$ = $1; }
			| TIME ZONE
				{
					throw ParserException("VARIABLE RESET");
				}
		;

generic_reset:
			var_name
				{
					throw ParserException("VARIABLE RESET");
				}
			| ALL
				{
					throw ParserException("VARIABLE RESET");
				}
		;

/*****************************************************************************
 *
 * SHOW variable;
 *
 *****************************************************************************/
VariableShowStmt:
			SHOW var_name
				{
					throw ParserException("VARIABLE SHOW");
				}
			| SHOW TIME ZONE
				{
					throw ParserException("VARIABLE SHOW");
				}
			| SHOW ALL
				{
					throw ParserException("VARIABLE SHOW");
				}
		;
