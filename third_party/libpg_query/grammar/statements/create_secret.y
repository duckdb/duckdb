/*****************************************************************************
 *
 * Create a secret
 *
 *****************************************************************************/
CreateSecretStmt:
			CREATE_P opt_persist SECRET opt_secret_name '(' copy_generic_opt_list ')'
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->persist_option = $2;
					n->secret_name = $4;
					n->options = $6;
					n->onconflict = PG_ERROR_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P opt_persist SECRET IF_P NOT EXISTS opt_secret_name '(' copy_generic_opt_list ')'
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->persist_option = $2;
					n->secret_name = $7;
					n->options = $9;
					n->onconflict = PG_IGNORE_ON_CONFLICT;
					$$ = (PGNode *)n;
				}
			| CREATE_P OR REPLACE opt_persist SECRET opt_secret_name '(' copy_generic_opt_list ')'
				{
					PGCreateSecretStmt *n = makeNode(PGCreateSecretStmt);
					n->persist_option = $4;
					n->secret_name = $6;
					n->options = $8;
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
        | IDENT                                     { $$ = $1; }
    ;
