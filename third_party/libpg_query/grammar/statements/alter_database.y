/*****************************************************************************
 *
 * Alter Database Statement
 *
 *****************************************************************************/
AlterDatabaseStmt:
			ALTER DATABASE ColId SET IDENT TO ColId
				{
					if (strcasecmp($5, "alias") != 0) {
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("expected SET ALIAS TO, got SET %s TO", $5),
								 parser_errposition(@5)));
					}
					PGAlterDatabaseStmt *n = makeNode(PGAlterDatabaseStmt);
					n->dbname = $3;
					n->new_name = $7;
					n->alter_type = PG_ALTER_DATABASE_RENAME;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER DATABASE IF_P EXISTS ColId SET IDENT TO ColId
				{
					if (strcasecmp($7, "alias") != 0) {
						ereport(ERROR,
								(errcode(PG_ERRCODE_SYNTAX_ERROR),
								 errmsg("expected SET ALIAS TO, got SET %s TO", $7),
								 parser_errposition(@7)));
					}
					PGAlterDatabaseStmt *n = makeNode(PGAlterDatabaseStmt);
					n->dbname = $5;
					n->new_name = $9;
					n->alter_type = PG_ALTER_DATABASE_RENAME;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
		;
