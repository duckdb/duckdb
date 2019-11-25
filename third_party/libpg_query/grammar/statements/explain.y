/*****************************************************************************
 *
 *		QUERY:
 *				EXPLAIN [ANALYZE] [VERBOSE] query
 *				EXPLAIN ( options ) query
 *
 *****************************************************************************/
ExplainStmt:
		EXPLAIN ExplainableStmt
				{
					PGExplainStmt *n = makeNode(PGExplainStmt);
					n->query = $2;
					n->options = NIL;
					$$ = (PGNode *) n;
				}
		| EXPLAIN analyze_keyword opt_verbose ExplainableStmt
				{
					PGExplainStmt *n = makeNode(PGExplainStmt);
					n->query = $4;
					n->options = list_make1(makeDefElem("analyze", NULL, @2));
					if ($3)
						n->options = lappend(n->options,
											 makeDefElem("verbose", NULL, @3));
					$$ = (PGNode *) n;
				}
		| EXPLAIN VERBOSE ExplainableStmt
				{
					PGExplainStmt *n = makeNode(PGExplainStmt);
					n->query = $3;
					n->options = list_make1(makeDefElem("verbose", NULL, @2));
					$$ = (PGNode *) n;
				}
		| EXPLAIN '(' explain_option_list ')' ExplainableStmt
				{
					PGExplainStmt *n = makeNode(PGExplainStmt);
					n->query = $5;
					n->options = $3;
					$$ = (PGNode *) n;
				}
		;


opt_verbose:
			VERBOSE									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;


explain_option_arg:
			opt_boolean_or_string	{ $$ = (PGNode *) makeString($1); }
			| NumericOnly			{ $$ = (PGNode *) $1; }
			| /* EMPTY */			{ $$ = NULL; }
		;


ExplainableStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt
			| CreateAsStmt					/* by default all are $$=$1 */
		;


NonReservedWord:	IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
		;


NonReservedWord_or_Sconst:
			NonReservedWord							{ $$ = $1; }
			| Sconst								{ $$ = $1; }
		;


explain_option_list:
			explain_option_elem
				{
					$$ = list_make1($1);
				}
			| explain_option_list ',' explain_option_elem
				{
					$$ = lappend($1, $3);
				}
		;


analyze_keyword:
			ANALYZE									{}
			| ANALYSE /* British */					{}
		;


opt_boolean_or_string:
			TRUE_P									{ $$ = (char*) "true"; }
			| FALSE_P								{ $$ = (char*) "false"; }
			| ON									{ $$ = (char*) "on"; }
			/*
			 * OFF is also accepted as a boolean value, but is handled by
			 * the NonReservedWord rule.  The action for booleans and strings
			 * is the same, so we don't need to distinguish them here.
			 */
			| NonReservedWord_or_Sconst				{ $$ = $1; }
		;


explain_option_elem:
			explain_option_name explain_option_arg
				{
					$$ = makeDefElem($1, $2, @1);
				}
		;


explain_option_name:
			NonReservedWord			{ $$ = $1; }
			| analyze_keyword		{ $$ = (char*) "analyze"; }
		;
