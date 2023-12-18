/*****************************************************************************
 *
 * Create a secret
 *
 *****************************************************************************/
CreateSecretStmt:
			CREATE_P opt_persist SECRET opt_secret_name opt_storage_specifier '(' copy_generic_opt_list ')'
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->persist_type = $2;
					n->secret_name = $4;
					n->secret_storage = $5;
					n->options = $7;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P opt_persist SECRET IF_P NOT EXISTS opt_secret_name opt_storage_specifier '(' copy_generic_opt_list ')'
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->persist_type = $2;
					n->secret_name = $7;
					n->secret_storage = $8;
					n->options = $10;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P OR REPLACE opt_persist SECRET opt_secret_name opt_storage_specifier '(' copy_generic_opt_list ')'
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->persist_type = $4;
					n->secret_name = $6;
					n->secret_storage = $7;
					n->options = $9;
					n->onconflict = PG_REPLACE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
		;

opt_secret_name:
	/* empty */ { $$ = NULL; }
	| ColId { $$ = $1; }
	;

opt_persist:
        /* empty */                                 { $$ = pstrdup("default"); }
        | TEMPORARY                                 { $$ = pstrdup("temporary"); }
        | PERSISTENT                                { $$ = pstrdup("persistent"); }
    ;

opt_storage_specifier:
        /* empty */                                 { $$ = pstrdup(""); }
        | IN_P IDENT                                { $$ = $2; }
    ;
