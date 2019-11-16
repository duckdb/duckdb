/*****************************************************************************
 *
 *		QUERY:
 *				VACUUM
 *				ANALYZE
 *
 *****************************************************************************/
VacuumStmt: VACUUM opt_full opt_freeze opt_verbose
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($3)
						n->options |= VACOPT_FREEZE;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose qualified_name
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($3)
						n->options |= VACOPT_FREEZE;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					n->relation = $5;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose AnalyzeStmt
				{
					VacuumStmt *n = (VacuumStmt *) $5;
					n->options |= VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($3)
						n->options |= VACOPT_FREEZE;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					$$ = (Node *)n;
				}
			| VACUUM '(' vacuum_option_list ')'
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM | $3;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *) n;
				}
			| VACUUM '(' vacuum_option_list ')' qualified_name opt_name_list
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM | $3;
					n->relation = $5;
					n->va_cols = $6;
					if (n->va_cols != NIL)	/* implies analyze */
						n->options |= VACOPT_ANALYZE;
					$$ = (Node *) n;
				}
		;


vacuum_option_elem:
			analyze_keyword		{ $$ = VACOPT_ANALYZE; }
			| VERBOSE			{ $$ = VACOPT_VERBOSE; }
			| FREEZE			{ $$ = VACOPT_FREEZE; }
			| FULL				{ $$ = VACOPT_FULL; }
			| IDENT
				{
					if (strcmp($1, "disable_page_skipping") == 0)
						$$ = VACOPT_DISABLE_PAGE_SKIPPING;
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("unrecognized VACUUM option \"%s\"", $1),
									 parser_errposition(@1)));
				}
		;


opt_full:	FULL									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;


vacuum_option_list:
			vacuum_option_elem								{ $$ = $1; }
			| vacuum_option_list ',' vacuum_option_elem		{ $$ = $1 | $3; }
		;


opt_freeze: FREEZE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;
