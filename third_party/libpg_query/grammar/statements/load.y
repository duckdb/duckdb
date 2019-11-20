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
					$$ = (PGNode *)n;
				}
		;


file_name:	Sconst									{ $$ = $1; };
