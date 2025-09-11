/*****************************************************************************
 *
 * Alter Database Statement
 *
 *****************************************************************************/
AlterDatabaseStmt:
			ALTER DATABASE ColId RENAME TO ColId
				{
					PGAlterDatabaseStmt *n = makeNode(PGAlterDatabaseStmt);
					n->dbname = $3;
					n->new_name = $6;
					n->alter_type = PG_ALTER_DATABASE_RENAME;
					n->missing_ok = false;
					$$ = (PGNode *)n;
				}
			| ALTER DATABASE IF_P EXISTS ColId RENAME TO ColId
				{
					PGAlterDatabaseStmt *n = makeNode(PGAlterDatabaseStmt);
					n->dbname = $5;
					n->new_name = $8;
					n->alter_type = PG_ALTER_DATABASE_RENAME;
					n->missing_ok = true;
					$$ = (PGNode *)n;
				}
		;
