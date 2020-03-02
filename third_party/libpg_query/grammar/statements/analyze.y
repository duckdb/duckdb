/*****************************************************************************
 *
 *		QUERY:
 *				VACUUM
 *				ANALYZE
 *
 *****************************************************************************/
AnalyzeStmt:
			analyze_keyword opt_verbose
				{
					PGVacuumStmt *n = makeNode(PGVacuumStmt);
					n->options = PG_VACOPT_ANALYZE;
					if ($2)
						n->options |= PG_VACOPT_VERBOSE;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (PGNode *)n;
				}
			| analyze_keyword opt_verbose qualified_name opt_name_list
				{
					PGVacuumStmt *n = makeNode(PGVacuumStmt);
					n->options = PG_VACOPT_ANALYZE;
					if ($2)
						n->options |= PG_VACOPT_VERBOSE;
					n->relation = $3;
					n->va_cols = $4;
					$$ = (PGNode *)n;
				}
		;
