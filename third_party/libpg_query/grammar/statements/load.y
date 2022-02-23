/*****************************************************************************
 *
 *		QUERY:
 *				LOAD "filename"
 *
 *****************************************************************************/
LoadStmt:	LOAD file_name
				{
					PGLoadStmt *n = makeNode(PGLoadStmt);
					n->filename = $2;
					n->load_type = PG_LOAD_TYPE_LOAD;
					$$ = (PGNode *)n;
				} |
				INSTALL file_name {
                    PGLoadStmt *n = makeNode(PGLoadStmt);
                    n->filename = $2;
                    n->load_type = PG_LOAD_TYPE_INSTALL;
                    $$ = (PGNode *)n;
				} |
				FORCE INSTALL file_name {
                      PGLoadStmt *n = makeNode(PGLoadStmt);
                      n->filename = $3;
                      n->load_type = PG_LOAD_TYPE_FORCE_INSTALL;
                      $$ = (PGNode *)n;
                }
		;

file_name:	Sconst								{ $$ = $1; } |
            ColId                               { $$ = $1; };
