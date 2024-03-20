/*****************************************************************************
 *
 * Drop a secret
 *
 *****************************************************************************/
DropSecretStmt:
			DROP opt_persist SECRET ColId opt_storage_drop_specifier
				{
					PGDropSecretStmt *n = makeNode(PGDropSecretStmt);
					n->persist_type = $2;
					n->secret_name = $4;
					n->secret_storage = $5;
					n->missing_ok  = false;
					$$ = (PGNode *)n;
				}
            | DROP opt_persist SECRET IF_P EXISTS ColId opt_storage_drop_specifier
                {
                    PGDropSecretStmt *n = makeNode(PGDropSecretStmt);
                    n->persist_type = $2;
                    n->secret_name = $6;
                    n->secret_storage = $7;
                    n->missing_ok  = true;
                    $$ = (PGNode *)n;
                }
		;

opt_storage_drop_specifier:
        /* empty */                                 { $$ = pstrdup(""); }
        | FROM IDENT                                { $$ = $2; }
    ;
