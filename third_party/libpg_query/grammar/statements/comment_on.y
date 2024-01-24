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
		;

comment_value:
			Sconst			                { $$ = makeStringConst($1, @1); }
			| NULL_P					    { $$ = makeNullAConst(@1); }

// TODO ensure these match what we support?
comment_on_type_any_name:
			TABLE									{ $$ = PG_OBJECT_TABLE; }
			| SEQUENCE								{ $$ = PG_OBJECT_SEQUENCE; }
			| FUNCTION								{ $$ = PG_OBJECT_FUNCTION; }
			| MACRO									{ $$ = PG_OBJECT_FUNCTION; }
			| MACRO TABLE                           { $$ = PG_OBJECT_TABLE_MACRO; }
			| VIEW									{ $$ = PG_OBJECT_VIEW; }
			| DATABASE								{ $$ = PG_OBJECT_DATABASE; }
			| MATERIALIZED VIEW						{ $$ = PG_OBJECT_MATVIEW; }
			| INDEX									{ $$ = PG_OBJECT_INDEX; }
			| COLLATION								{ $$ = PG_OBJECT_COLLATION; }
			| COLUMN								{ $$ = PG_OBJECT_COLUMN; }
			| CONVERSION_P							{ $$ = PG_OBJECT_CONVERSION; }
			| SCHEMA								{ $$ = PG_OBJECT_SCHEMA; }
			| STATISTICS							{ $$ = PG_OBJECT_STATISTIC_EXT; }
			| TEXT_P SEARCH PARSER					{ $$ = PG_OBJECT_TSPARSER; }
			| TEXT_P SEARCH DICTIONARY				{ $$ = PG_OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH TEMPLATE				{ $$ = PG_OBJECT_TSTEMPLATE; }
			| TEXT_P SEARCH CONFIGURATION			{ $$ = PG_OBJECT_TSCONFIGURATION; }
			| TYPE_P								{ $$ = PG_OBJECT_TYPE; }
		;
