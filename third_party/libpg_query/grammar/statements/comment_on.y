/*****************************************************************************
 *
 * Create a Comment
 *
 *****************************************************************************/
CommentOnStmt:
			COMMENT ON comment_on_type_any_name qualified_name IS comment_value
				{
					PGCommentOnStmt *n = makeNode(PGCommentOnStmt);
					n->object_type = $3;
					n->name = $4;
					n->value = $6;
					$$ = (PGNode *)n;
				}
            | COMMENT ON COLUMN a_expr IS comment_value
                {
                    PGCommentOnStmt *n = makeNode(PGCommentOnStmt);
                    n->object_type = PG_OBJECT_COLUMN;
                    n->column_expr = $4;
                    n->value = $6;
                    $$ = (PGNode *)n;
                }
		;

comment_value:
			Sconst			                { $$ = makeStringConst($1, @1); }
			| NULL_P					    { $$ = makeNullAConst(@1); }

comment_on_type_any_name:
			TABLE									{ $$ = PG_OBJECT_TABLE; }
			| SEQUENCE								{ $$ = PG_OBJECT_SEQUENCE; }
			| FUNCTION								{ $$ = PG_OBJECT_FUNCTION; }
			| MACRO									{ $$ = PG_OBJECT_FUNCTION; }
			| MACRO TABLE                           { $$ = PG_OBJECT_TABLE_MACRO; }
			| VIEW									{ $$ = PG_OBJECT_VIEW; }
			| DATABASE								{ $$ = PG_OBJECT_DATABASE; }
			| INDEX									{ $$ = PG_OBJECT_INDEX; }
			| SCHEMA								{ $$ = PG_OBJECT_SCHEMA; }
			| TYPE_P								{ $$ = PG_OBJECT_TYPE; }
		;
