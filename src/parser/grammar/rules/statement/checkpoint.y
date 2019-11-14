/*****************************************************************************
 *
 * CHECKPOINT, VACUUM and ANALYZE
 *
 *****************************************************************************/
/*****************************************************************************
 *
 * CHECKPOINT
 *
 *****************************************************************************/
CheckPointStmt:
			CHECKPOINT
				{
					throw ParserException("Unsupported: Checkpoint");
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				VACUUM
 *				ANALYZE
 *
 *****************************************************************************/
VacuumStmt: VACUUM opt_full opt_freeze opt_verbose opt_analyze opt_vacuum_relation_list
				{
					throw ParserException("FIXME: vacuum");
				}
			| VACUUM '(' vac_analyze_option_list ')' opt_vacuum_relation_list
				{
					throw ParserException("FIXME: vacuum");
				}
		;

AnalyzeStmt: analyze_keyword opt_verbose opt_vacuum_relation_list
				{
					throw ParserException("FIXME: analyze");
				}
			| analyze_keyword '(' vac_analyze_option_list ')' opt_vacuum_relation_list
				{
					throw ParserException("FIXME: analyze");
				}
		;

vac_analyze_option_list:
			vac_analyze_option_elem
				{
					$$ = list_make1($1);
				}
			| vac_analyze_option_list ',' vac_analyze_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

analyze_keyword:
			ANALYZE									{}
			| ANALYSE /* British */					{}
		;

vac_analyze_option_elem:
			vac_analyze_option_name vac_analyze_option_arg
				{
					$$ = makeDefElem($1, $2, @1);
				}
		;

vac_analyze_option_name:
			NonReservedWord							{ $$ = $1; }
			| analyze_keyword						{ $$ = "analyze"; }
		;

vac_analyze_option_arg:
			opt_boolean_or_string					{ $$ = (Node *) makeString($1); }
			| NumericOnly			{ $$ = (Node *) $1; }
			| /* EMPTY */		 					{ $$ = NULL; }
		;

opt_analyze:
			analyze_keyword							{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;

opt_full:	FULL									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;

opt_freeze: FREEZE									{ $$ = true; }
			| /*EMPTY*/								{ $$ = false; }
		;

vacuum_relation:
			qualified_name opt_name_list
				{
					$$ = (Node *) makeVacuumRelation($1, InvalidOid, $2);
				}
		;

vacuum_relation_list:
			vacuum_relation
					{ $$ = list_make1($1); }
			| vacuum_relation_list ',' vacuum_relation
					{ $$ = lappend($1, $3); }
		;

opt_vacuum_relation_list:
			vacuum_relation_list					{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
