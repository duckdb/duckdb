/*****************************************************************************
 *
 *		QUERY :
 *				CREATE DATABASE [ IF EXISTS ] itemname
 *
 *****************************************************************************/
CreateDatabaseStmt:
                CREATE_P DATABASE database_name
				{
					PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
					n->name = $3;
					$$ = (PGNode *)n;
				}
				| CREATE_P opt_extension_name DATABASE database_name
				{
				    PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
				    n->extension = $2;
                    n->name = $4;
                    $$ = (PGNode *)n;
				}
				| CREATE_P DATABASE IF_P NOT EXISTS database_name
				{
					PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
					n->name = $6;
					$$ = (PGNode *)n;
				}
				| CREATE_P OR REPLACE DATABASE database_name
				{
					PGCreateDatabaseStmt *n = makeNode(PGCreateDatabaseStmt);
					n->name = $5;
					$$ = (PGNode *)n;
				}
		;

database_name:
				Sconst				{ $$ = $1; }
		;

opt_extension_name:
				Sconst				{ $$ = $1; }
		;
