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
					n->install = 0;
					$$ = (PGNode *)n;
				} |
				INSTALL file_name {
                    PGLoadStmt *n = makeNode(PGLoadStmt);
                    n->filename = $2;
                    n->install = 1;
                    $$ = (PGNode *)n;
				}
		;

file_name:	Sconst									{ $$ = $1; };
