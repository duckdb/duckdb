/*****************************************************************************
 *
 * Manipulate a schema
 *
 *****************************************************************************/
CreateSchemaStmt:
			CREATE_P SCHEMA ColId OptSchemaEltList
				{
					PGCreateSchemaStmt *n = makeNode(PGCreateSchemaStmt);
					/* ...but not both */
					n->schemaname = $3;
					n->schemaElts = $4;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P SCHEMA IF_P NOT EXISTS ColId OptSchemaEltList
				{
					PGCreateSchemaStmt *n = makeNode(PGCreateSchemaStmt);
					/* ...but not here */
					n->schemaname = $6;
					if ($7 != NIL)
						ereport(ERROR,
								(errcode(PG_ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("CREATE SCHEMA IF NOT EXISTS cannot include schema elements"),
								 parser_errposition(@7)));
					n->schemaElts = $7;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
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
