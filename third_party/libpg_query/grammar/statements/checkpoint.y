/*
 * Checkpoint statement
 */
CheckPointStmt:
			CHECKPOINT
				{
					PGCheckPointStmt *n = makeNode(PGCheckPointStmt);
					$$ = (PGNode *)n;
				}
		;
