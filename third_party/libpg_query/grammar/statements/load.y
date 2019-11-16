/*****************************************************************************
 *
 *		QUERY:
 *				LOAD "filename"
 *
 *****************************************************************************/
LoadStmt:	LOAD file_name
				{
					LoadStmt *n = makeNode(LoadStmt);
					n->filename = $2;
					$$ = (Node *)n;
				}
		;


file_name:	Sconst									{ $$ = $1; };
