UseStmt:
			USE_P qualified_name
				{
					PGUseStmt *n = makeNode(PGUseStmt);
					n->name = $2;
					$$ = (PGNode *) n;
				}
		;
