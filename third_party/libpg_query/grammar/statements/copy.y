CopyStmt:	COPY opt_binary qualified_name opt_column_list opt_oids
			copy_from opt_program copy_file_name copy_delimiter opt_with copy_options
				{
					CopyStmt *n = makeNode(CopyStmt);
					n->relation = $3;
					n->query = NULL;
					n->attlist = $4;
					n->is_from = $6;
					n->is_program = $7;
					n->filename = $8;

					if (n->is_program && n->filename == NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("STDIN/STDOUT not allowed with PROGRAM"),
								 parser_errposition(@8)));

					n->options = NIL;
					/* Concatenate user-supplied flags */
					if ($2)
						n->options = lappend(n->options, $2);
					if ($5)
						n->options = lappend(n->options, $5);
					if ($9)
						n->options = lappend(n->options, $9);
					if ($11)
						n->options = list_concat(n->options, $11);
					$$ = (Node *)n;
				}
			| COPY '(' PreparableStmt ')' TO opt_program copy_file_name opt_with copy_options
				{
					CopyStmt *n = makeNode(CopyStmt);
					n->relation = NULL;
					n->query = $3;
					n->attlist = NIL;
					n->is_from = false;
					n->is_program = $6;
					n->filename = $7;
					n->options = $9;

					if (n->is_program && n->filename == NULL)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("STDIN/STDOUT not allowed with PROGRAM"),
								 parser_errposition(@5)));

					$$ = (Node *)n;
				}
		;


copy_from:
			FROM									{ $$ = TRUE; }
			| TO									{ $$ = FALSE; }
		;


copy_delimiter:
			opt_using DELIMITERS Sconst
				{
					$$ = makeDefElem("delimiter", (Node *)makeString($3), @2);
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;


copy_generic_opt_arg_list:
			  copy_generic_opt_arg_list_item
				{
					$$ = list_make1($1);
				}
			| copy_generic_opt_arg_list ',' copy_generic_opt_arg_list_item
				{
					$$ = lappend($1, $3);
				}
		;


opt_using:
			USING									{}
			| /*EMPTY*/								{}
		;


opt_as:		AS										{}
			| /* EMPTY */							{}
		;


opt_program:
			PROGRAM									{ $$ = TRUE; }
			| /* EMPTY */							{ $$ = FALSE; }
		;


copy_options: copy_opt_list							{ $$ = $1; }
			| '(' copy_generic_opt_list ')'			{ $$ = $2; }
		;


copy_generic_opt_arg:
			opt_boolean_or_string			{ $$ = (Node *) makeString($1); }
			| NumericOnly					{ $$ = (Node *) $1; }
			| '*'							{ $$ = (Node *) makeNode(A_Star); }
			| '(' copy_generic_opt_arg_list ')'		{ $$ = (Node *) $2; }
			| /* EMPTY */					{ $$ = NULL; }
		;


copy_generic_opt_elem:
			ColLabel copy_generic_opt_arg
				{
					$$ = makeDefElem($1, $2, @1);
				}
		;


opt_oids:
			WITH OIDS
				{
					$$ = makeDefElem("oids", (Node *)makeInteger(TRUE), @1);
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;


copy_opt_list:
			copy_opt_list copy_opt_item				{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;


opt_binary:
			BINARY
				{
					$$ = makeDefElem("format", (Node *)makeString("binary"), @1);
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;


copy_opt_item:
			BINARY
				{
					$$ = makeDefElem("format", (Node *)makeString("binary"), @1);
				}
			| OIDS
				{
					$$ = makeDefElem("oids", (Node *)makeInteger(TRUE), @1);
				}
			| FREEZE
				{
					$$ = makeDefElem("freeze", (Node *)makeInteger(TRUE), @1);
				}
			| DELIMITER opt_as Sconst
				{
					$$ = makeDefElem("delimiter", (Node *)makeString($3), @1);
				}
			| NULL_P opt_as Sconst
				{
					$$ = makeDefElem("null", (Node *)makeString($3), @1);
				}
			| CSV
				{
					$$ = makeDefElem("format", (Node *)makeString("csv"), @1);
				}
			| HEADER_P
				{
					$$ = makeDefElem("header", (Node *)makeInteger(TRUE), @1);
				}
			| QUOTE opt_as Sconst
				{
					$$ = makeDefElem("quote", (Node *)makeString($3), @1);
				}
			| ESCAPE opt_as Sconst
				{
					$$ = makeDefElem("escape", (Node *)makeString($3), @1);
				}
			| FORCE QUOTE columnList
				{
					$$ = makeDefElem("force_quote", (Node *)$3, @1);
				}
			| FORCE QUOTE '*'
				{
					$$ = makeDefElem("force_quote", (Node *)makeNode(A_Star), @1);
				}
			| FORCE NOT NULL_P columnList
				{
					$$ = makeDefElem("force_not_null", (Node *)$4, @1);
				}
			| FORCE NULL_P columnList
				{
					$$ = makeDefElem("force_null", (Node *)$3, @1);
				}
			| ENCODING Sconst
				{
					$$ = makeDefElem("encoding", (Node *)makeString($2), @1);
				}
		;


copy_generic_opt_arg_list_item:
			opt_boolean_or_string	{ $$ = (Node *) makeString($1); }
		;



copy_file_name:
			Sconst									{ $$ = $1; }
			| STDIN									{ $$ = NULL; }
			| STDOUT								{ $$ = NULL; }
		;


copy_generic_opt_list:
			copy_generic_opt_elem
				{
					$$ = list_make1($1);
				}
			| copy_generic_opt_list ',' copy_generic_opt_elem
				{
					$$ = lappend($1, $3);
				}
		;
