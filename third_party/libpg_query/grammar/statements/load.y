/*****************************************************************************
 *
 *		QUERY:
 *				LOAD "filename"
 *
 *****************************************************************************/
LoadStmt:	LOAD file_name
				{
					PGLoadStmt *n = makeNode(PGLoadStmt);
					n->filename = $2;
					n->repository = "";
					n->version = "";
					n->load_type = PG_LOAD_TYPE_LOAD;
					$$ = (PGNode *)n;
				} |
				INSTALL file_name opt_from_repo opt_ext_version {
                    PGLoadStmt *n = makeNode(PGLoadStmt);
                    n->filename = $2;
                    n->repository = $3;
                    n->version = $4;
                    n->load_type = PG_LOAD_TYPE_INSTALL;
                    $$ = (PGNode *)n;
				} |
				FORCE INSTALL file_name opt_from_repo opt_ext_version {
                      PGLoadStmt *n = makeNode(PGLoadStmt);
                      n->filename = $3;
                      n->repository = $4;
                      n->version = $5;
                      n->load_type = PG_LOAD_TYPE_FORCE_INSTALL;
                      $$ = (PGNode *)n;
                }
		;

file_name:	Sconst								{ $$ = $1; } |
            ColId                               { $$ = $1; };

repo_path:	Sconst								{ $$ = $1; } |
            ColId                               { $$ = $1; };

opt_from_repo:
               /* empty */                      { $$ = ""; } |
                FROM Sconst						{ $$ = $2; } |
                FROM ColId                      { $$ = $2; };

opt_ext_version:
               /* empty */                      { $$ = ""; } |
                VERSION_P Sconst				{ $$ = $2; } |
                VERSION_P ColId                 { $$ = $2; };
