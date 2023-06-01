/*****************************************************************************
 *
 * Manipulate a schema
 *
 *****************************************************************************/
CreateSchemaStmt:
			CREATE_P SCHEMA qualified_name OptSchemaEltList
				{
					PGCreateSchemaStmt *n = makeNode(PGCreateSchemaStmt);
					if ($3->catalogname) {
						ereport(ERROR,
								(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE SCHEMA too many dots: expected \"catalog.schema\" or \"schema\""),
								 parser_errposition(@3)));
					}
					if ($3->schemaname) {
						n->catalogname = $3->schemaname;
						n->schemaname = $3->relname;
					} else {
						n->schemaname = $3->relname;
					}
					n->schemaElts = $4;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P SCHEMA IF_P NOT EXISTS qualified_name OptSchemaEltList
				{
					PGCreateSchemaStmt *n = makeNode(PGCreateSchemaStmt);
					if ($6->catalogname) {
						ereport(ERROR,
								(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE SCHEMA too many dots: expected \"catalog.schema\" or \"schema\""),
								 parser_errposition(@6)));
					}
					if ($6->schemaname) {
						n->catalogname = $6->schemaname;
						n->schemaname = $6->relname;
					} else {
						n->schemaname = $6->relname;
					}
					if ($7 != NIL)
						ereport(ERROR,
								(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE SCHEMA IF NOT EXISTS cannot include schema elements"),
								 parser_errposition(@7)));
					n->schemaElts = $7;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P OR REPLACE SCHEMA qualified_name OptSchemaEltList
				{
					PGCreateSchemaStmt *n = makeNode(PGCreateSchemaStmt);
					if ($5->catalogname) {
						ereport(ERROR,
								(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE SCHEMA too many dots: expected \"catalog.schema\" or \"schema\""),
								 parser_errposition(@5)));
					}
					if ($5->schemaname) {
						n->catalogname = $5->schemaname;
						n->schemaname = $5->relname;
					} else {
						n->schemaname = $5->relname;
					}
					n->schemaElts = $6;
					n->onconflict = PG_REPLACE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		;


OptSchemaEltList:
			OptSchemaEltList schema_stmt
				{
					if (@$ < 0)			/* see comments for YYLLOC_DEFAULT */
						@$ = @2;
					$$ = lappend($1, $2);
				}
			| /* EMPTY */
				{ $$ = NIL; }
		;


schema_stmt:
			CreateStmt
			| IndexStmt
			| CreateSeqStmt
			| ViewStmt
		;
