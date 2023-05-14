/*****************************************************************************
 *
 *		QUERY :
 *				CREATE DATABASE [ IF EXISTS ] itemname
 *				CREATE DATABASE [ IF EXISTS ] itemname FROM path
 *
 *****************************************************************************/
CreateDatabaseStmt:
                CREATE_P DATABASE qualified_name
				{
					PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
					n->name = $3;
					$$ = (PGNode *)n;
				}
				| CREATE_P opt_extension_name DATABASE qualified_name
				{
				    PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
				    n->extension = $2;
                    n->name = $4;
                    $$ = (PGNode *)n;
				}
				| CREATE_P DATABASE IF_P NOT EXISTS qualified_name
				{
					PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
					n->name = $6;
					$$ = (PGNode *)n;
				}
				| CREATE_P OR REPLACE DATABASE qualified_name
				{
					PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
					n->name = $5;
					$$ = (PGNode *)n;
				}
                | CREATE_P DATABASE qualified_name FROM Sconst
                {
                    PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
                    n->name = $3;
                    n->path = $5;
                    $$ = (PGNode *)n;
                }
				| CREATE_P DATABASE IF_P NOT EXISTS qualified_name FROM Sconst
				{
					PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
					n->name = $6;
					n->path = $8;
					$$ = (PGNode *)n;
				}
				| CREATE_P OR REPLACE DATABASE qualified_name FROM Sconst
				{
					PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
					n->name = $5;
					n->path = $7;
					$$ = (PGNode *)n;
				}
		;


opt_extension_name:
				Sconst				{ $$ = $1; }
		;
