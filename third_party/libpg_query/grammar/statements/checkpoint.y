/*
 * Checkpoint statement
 */
CheckPointStmt:
			FORCE CHECKPOINT
				{
					PGCheckPointStmt *n = makeNode(PGCheckPointStmt);
					n->force = true;
					$$ = (PGNode *)n;
				}
			| CHECKPOINT
				{
					PGCheckPointStmt *n = makeNode(PGCheckPointStmt);
					n->force = false;
					$$ = (PGNode *)n;
				}
		;
