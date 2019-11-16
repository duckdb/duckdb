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
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE;
					if ($2)
						n->options |= VACOPT_VERBOSE;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| analyze_keyword opt_verbose qualified_name opt_name_list
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE;
					if ($2)
						n->options |= VACOPT_VERBOSE;
					n->relation = $3;
					n->va_cols = $4;
					$$ = (Node *)n;
				}
		;
