/*****************************************************************************
 *
 * Drop a secret
 *
 *****************************************************************************/
DropSecretStmt:
			DROP SECRET ColId
				{
					PGDropSecretStmt *n = makeNode(PGDropSecretStmt);
					n->secret_name = $3;
					n->missing_ok  = false;
					$$ = (PGNode *)n;
				}
            | DROP SECRET ColId IF_P EXISTS
                {
                    PGDropSecretStmt *n = makeNode(PGDropSecretStmt);
                    n->secret_name = $3;
                    n->missing_ok  = true;
                    $$ = (PGNode *)n;
                }
		;
