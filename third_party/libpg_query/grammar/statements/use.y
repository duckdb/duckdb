UseStmt:
			USE_P ColId
				{
					PGUseStmt *n = makeNode(PGUseStmt);
					n->name = $2;
					$$ = (PGNode *) n;
				}
		;
