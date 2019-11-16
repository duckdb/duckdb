/*
 * Checkpoint statement
 */
CheckPointStmt:
			CHECKPOINT
				{
					CheckPointStmt *n = makeNode(CheckPointStmt);
					$$ = (Node *)n;
				}
		;
