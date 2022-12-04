/*****************************************************************************
 *
 * Attach Statement
 *
 *****************************************************************************/
AttachStmt:
				ATTACH DATABASE Sconst AS ColId
				{
					PGAttachStmt *n = makeNode(PGAttachStmt);
					n->path = $3;
					n->name = $5;
					$$ = (PGNode *)n;
				}
		;
