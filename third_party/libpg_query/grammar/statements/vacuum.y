/*****************************************************************************
 *
 *		QUERY:
 *				VACUUM
 *				ANALYZE
 *
 *****************************************************************************/
VacuumStmt: VACUUM opt_full opt_freeze opt_verbose
				{
					PGVacuumStmt *n = makeNode(PGVacuumStmt);
					n->options = PG_VACOPT_VACUUM;
					if ($2)
						n->options |= PG_VACOPT_FULL;
					if ($3)
						n->options |= PG_VACOPT_FREEZE;
					if ($4)
						n->options |= PG_VACOPT_VERBOSE;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (PGNode *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose qualified_name opt_name_list
				{
					PGVacuumStmt *n = makeNode(PGVacuumStmt);
					n->options = PG_VACOPT_VACUUM;
					if ($2)
						n->options |= PG_VACOPT_FULL;
					if ($3)
						n->options |= PG_VACOPT_FREEZE;
					if ($4)
						n->options |= PG_VACOPT_VERBOSE;
					n->relation = $5;
					n->va_cols = $6;
					$$ = (PGNode *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose AnalyzeStmt
				{
					PGVacuumStmt *n = (PGVacuumStmt *) $5;
					n->options |= PG_VACOPT_VACUUM;
					if ($2)
						n->options |= PG_VACOPT_FULL;
					if ($3)
						n->options |= PG_VACOPT_FREEZE;
					if ($4)
						n->options |= PG_VACOPT_VERBOSE;
					$$ = (PGNode *)n;
				}
			| VACUUM '(' vacuum_option_list ')'
				{
					PGVacuumStmt *n = makeNode(PGVacuumStmt);
					n->options = PG_VACOPT_VACUUM | $3;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (PGNode *) n;
				}
			| VACUUM '(' vacuum_option_list ')' qualified_name opt_name_list
				{
					PGVacuumStmt *n = makeNode(PGVacuumStmt);
					n->options = PG_VACOPT_VACUUM | $3;
					n->relation = $5;
					n->va_cols = $6;
					if (n->va_cols != NIL)	/* implies analyze */
						n->options |= PG_VACOPT_ANALYZE;
					$$ = (PGNode *) n;
				}
		;


vacuum_option_elem:
			analyze_keyword		{ $$ = PG_VACOPT_ANALYZE; }
			| VERBOSE			{ $$ = PG_VACOPT_VERBOSE; }
			| FREEZE			{ $$ = PG_VACOPT_FREEZE; }
			| FULL				{ $$ = PG_VACOPT_FULL; }
			| IDENT
				{
					if (strcmp($1, "disable_page_skipping") == 0)
						$$ = PG_VACOPT_DISABLE_PAGE_SKIPPING;
					else
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
							 errmsg("unrecognized VACUUM option \"%s\"", $1),
									 parser_errposition(@1)));
				}
		;


opt_full:	FULL									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;


vacuum_option_list:
			vacuum_option_elem								{ $$ = $1; }
			| vacuum_option_list ',' vacuum_option_elem		{ $$ = $1 | $3; }
		;


opt_freeze: FREEZE									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;
