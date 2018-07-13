/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     IDENT = 258,
     FCONST = 259,
     SCONST = 260,
     BCONST = 261,
     XCONST = 262,
     Op = 263,
     ICONST = 264,
     PARAM = 265,
     TYPECAST = 266,
     DOT_DOT = 267,
     COLON_EQUALS = 268,
     EQUALS_GREATER = 269,
     LESS_EQUALS = 270,
     GREATER_EQUALS = 271,
     NOT_EQUALS = 272,
     ABORT_P = 273,
     ABSOLUTE_P = 274,
     ACCESS = 275,
     ACTION = 276,
     ADD_P = 277,
     ADMIN = 278,
     AFTER = 279,
     AGGREGATE = 280,
     ALL = 281,
     ALSO = 282,
     ALTER = 283,
     ALWAYS = 284,
     ANALYSE = 285,
     ANALYZE = 286,
     AND = 287,
     ANY = 288,
     ARRAY = 289,
     AS = 290,
     ASC = 291,
     ASSERTION = 292,
     ASSIGNMENT = 293,
     ASYMMETRIC = 294,
     AT = 295,
     ATTRIBUTE = 296,
     AUTHORIZATION = 297,
     BACKWARD = 298,
     BEFORE = 299,
     BEGIN_P = 300,
     BETWEEN = 301,
     BIGINT = 302,
     BINARY = 303,
     BIT = 304,
     BOOLEAN_P = 305,
     BOTH = 306,
     BY = 307,
     CACHE = 308,
     CALLED = 309,
     CASCADE = 310,
     CASCADED = 311,
     CASE = 312,
     CAST = 313,
     CATALOG_P = 314,
     CHAIN = 315,
     CHAR_P = 316,
     CHARACTER = 317,
     CHARACTERISTICS = 318,
     CHECK = 319,
     CHECKPOINT = 320,
     CLASS = 321,
     CLOSE = 322,
     CLUSTER = 323,
     COALESCE = 324,
     COLLATE = 325,
     COLLATION = 326,
     COLUMN = 327,
     COMMENT = 328,
     COMMENTS = 329,
     COMMIT = 330,
     COMMITTED = 331,
     CONCURRENTLY = 332,
     CONFIGURATION = 333,
     CONFLICT = 334,
     CONNECTION = 335,
     CONSTRAINT = 336,
     CONSTRAINTS = 337,
     CONTENT_P = 338,
     CONTINUE_P = 339,
     CONVERSION_P = 340,
     COPY = 341,
     COST = 342,
     CREATE = 343,
     CROSS = 344,
     CSV = 345,
     CUBE = 346,
     CURRENT_P = 347,
     CURRENT_CATALOG = 348,
     CURRENT_DATE = 349,
     CURRENT_ROLE = 350,
     CURRENT_SCHEMA = 351,
     CURRENT_TIME = 352,
     CURRENT_TIMESTAMP = 353,
     CURRENT_USER = 354,
     CURSOR = 355,
     CYCLE = 356,
     DATA_P = 357,
     DATABASE = 358,
     DAY_P = 359,
     DEALLOCATE = 360,
     DEC = 361,
     DECIMAL_P = 362,
     DECLARE = 363,
     DEFAULT = 364,
     DEFAULTS = 365,
     DEFERRABLE = 366,
     DEFERRED = 367,
     DEFINER = 368,
     DELETE_P = 369,
     DELIMITER = 370,
     DELIMITERS = 371,
     DESC = 372,
     DICTIONARY = 373,
     DISABLE_P = 374,
     DISCARD = 375,
     DISTINCT = 376,
     DO = 377,
     DOCUMENT_P = 378,
     DOMAIN_P = 379,
     DOUBLE_P = 380,
     DROP = 381,
     EACH = 382,
     ELSE = 383,
     ENABLE_P = 384,
     ENCODING = 385,
     ENCRYPTED = 386,
     END_P = 387,
     ENUM_P = 388,
     ESCAPE = 389,
     EVENT = 390,
     EXCEPT = 391,
     EXCLUDE = 392,
     EXCLUDING = 393,
     EXCLUSIVE = 394,
     EXECUTE = 395,
     EXISTS = 396,
     EXPLAIN = 397,
     EXTENSION = 398,
     EXTERNAL = 399,
     EXTRACT = 400,
     FALSE_P = 401,
     FAMILY = 402,
     FETCH = 403,
     FILTER = 404,
     FIRST_P = 405,
     FLOAT_P = 406,
     FOLLOWING = 407,
     FOR = 408,
     FORCE = 409,
     FOREIGN = 410,
     FORWARD = 411,
     FREEZE = 412,
     FROM = 413,
     FULL = 414,
     FUNCTION = 415,
     FUNCTIONS = 416,
     GLOBAL = 417,
     GRANT = 418,
     GRANTED = 419,
     GREATEST = 420,
     GROUP_P = 421,
     GROUPING = 422,
     HANDLER = 423,
     HAVING = 424,
     HEADER_P = 425,
     HOLD = 426,
     HOUR_P = 427,
     IDENTITY_P = 428,
     IF_P = 429,
     ILIKE = 430,
     IMMEDIATE = 431,
     IMMUTABLE = 432,
     IMPLICIT_P = 433,
     IMPORT_P = 434,
     IN_P = 435,
     INCLUDING = 436,
     INCREMENT = 437,
     INDEX = 438,
     INDEXES = 439,
     INHERIT = 440,
     INHERITS = 441,
     INITIALLY = 442,
     INLINE_P = 443,
     INNER_P = 444,
     INOUT = 445,
     INPUT_P = 446,
     INSENSITIVE = 447,
     INSERT = 448,
     INSTEAD = 449,
     INT_P = 450,
     INTEGER = 451,
     INTERSECT = 452,
     INTERVAL = 453,
     INTO = 454,
     INVOKER = 455,
     IS = 456,
     ISNULL = 457,
     ISOLATION = 458,
     JOIN = 459,
     KEY = 460,
     LABEL = 461,
     LANGUAGE = 462,
     LARGE_P = 463,
     LAST_P = 464,
     LATERAL_P = 465,
     LEADING = 466,
     LEAKPROOF = 467,
     LEAST = 468,
     LEFT = 469,
     LEVEL = 470,
     LIKE = 471,
     LIMIT = 472,
     LISTEN = 473,
     LOAD = 474,
     LOCAL = 475,
     LOCALTIME = 476,
     LOCALTIMESTAMP = 477,
     LOCATION = 478,
     LOCK_P = 479,
     LOCKED = 480,
     LOGGED = 481,
     MAPPING = 482,
     MATCH = 483,
     MATERIALIZED = 484,
     MAXVALUE = 485,
     MINUTE_P = 486,
     MINVALUE = 487,
     MODE = 488,
     MONTH_P = 489,
     MOVE = 490,
     NAME_P = 491,
     NAMES = 492,
     NATIONAL = 493,
     NATURAL = 494,
     NCHAR = 495,
     NEXT = 496,
     NO = 497,
     NONE = 498,
     NOT = 499,
     NOTHING = 500,
     NOTIFY = 501,
     NOTNULL = 502,
     NOWAIT = 503,
     NULL_P = 504,
     NULLIF = 505,
     NULLS_P = 506,
     NUMERIC = 507,
     OBJECT_P = 508,
     OF = 509,
     OFF = 510,
     OFFSET = 511,
     OIDS = 512,
     ON = 513,
     ONLY = 514,
     OPERATOR = 515,
     OPTION = 516,
     OPTIONS = 517,
     OR = 518,
     ORDER = 519,
     ORDINALITY = 520,
     OUT_P = 521,
     OUTER_P = 522,
     OVER = 523,
     OVERLAPS = 524,
     OVERLAY = 525,
     OWNED = 526,
     OWNER = 527,
     PARSER = 528,
     PARTIAL = 529,
     PARTITION = 530,
     PASSING = 531,
     PASSWORD = 532,
     PLACING = 533,
     PLANS = 534,
     POLICY = 535,
     POSITION = 536,
     PRECEDING = 537,
     PRECISION = 538,
     PRESERVE = 539,
     PREPARE = 540,
     PREPARED = 541,
     PRIMARY = 542,
     PRIOR = 543,
     PRIVILEGES = 544,
     PROCEDURAL = 545,
     PROCEDURE = 546,
     PROGRAM = 547,
     QUOTE = 548,
     RANGE = 549,
     READ = 550,
     REAL = 551,
     REASSIGN = 552,
     RECHECK = 553,
     RECURSIVE = 554,
     REF = 555,
     REFERENCES = 556,
     REFRESH = 557,
     REINDEX = 558,
     RELATIVE_P = 559,
     RELEASE = 560,
     RENAME = 561,
     REPEATABLE = 562,
     REPLACE = 563,
     REPLICA = 564,
     RESET = 565,
     RESTART = 566,
     RESTRICT = 567,
     RETURNING = 568,
     RETURNS = 569,
     REVOKE = 570,
     RIGHT = 571,
     ROLE = 572,
     ROLLBACK = 573,
     ROLLUP = 574,
     ROW = 575,
     ROWS = 576,
     RULE = 577,
     SAVEPOINT = 578,
     SCHEMA = 579,
     SCROLL = 580,
     SEARCH = 581,
     SECOND_P = 582,
     SECURITY = 583,
     SELECT = 584,
     SEQUENCE = 585,
     SEQUENCES = 586,
     SERIALIZABLE = 587,
     SERVER = 588,
     SESSION = 589,
     SESSION_USER = 590,
     SET = 591,
     SETS = 592,
     SETOF = 593,
     SHARE = 594,
     SHOW = 595,
     SIMILAR = 596,
     SIMPLE = 597,
     SKIP = 598,
     SMALLINT = 599,
     SNAPSHOT = 600,
     SOME = 601,
     SQL_P = 602,
     STABLE = 603,
     STANDALONE_P = 604,
     START = 605,
     STATEMENT = 606,
     STATISTICS = 607,
     STDIN = 608,
     STDOUT = 609,
     STORAGE = 610,
     STRICT_P = 611,
     STRIP_P = 612,
     SUBSTRING = 613,
     SYMMETRIC = 614,
     SYSID = 615,
     SYSTEM_P = 616,
     TABLE = 617,
     TABLES = 618,
     TABLESAMPLE = 619,
     TABLESPACE = 620,
     TEMP = 621,
     TEMPLATE = 622,
     TEMPORARY = 623,
     TEXT_P = 624,
     THEN = 625,
     TIME = 626,
     TIMESTAMP = 627,
     TO = 628,
     TRAILING = 629,
     TRANSACTION = 630,
     TRANSFORM = 631,
     TREAT = 632,
     TRIGGER = 633,
     TRIM = 634,
     TRUE_P = 635,
     TRUNCATE = 636,
     TRUSTED = 637,
     TYPE_P = 638,
     TYPES_P = 639,
     UNBOUNDED = 640,
     UNCOMMITTED = 641,
     UNENCRYPTED = 642,
     UNION = 643,
     UNIQUE = 644,
     UNKNOWN = 645,
     UNLISTEN = 646,
     UNLOGGED = 647,
     UNTIL = 648,
     UPDATE = 649,
     USER = 650,
     USING = 651,
     VACUUM = 652,
     VALID = 653,
     VALIDATE = 654,
     VALIDATOR = 655,
     VALUE_P = 656,
     VALUES = 657,
     VARCHAR = 658,
     VARIADIC = 659,
     VARYING = 660,
     VERBOSE = 661,
     VERSION_P = 662,
     VIEW = 663,
     VIEWS = 664,
     VOLATILE = 665,
     WHEN = 666,
     WHERE = 667,
     WHITESPACE_P = 668,
     WINDOW = 669,
     WITH = 670,
     WITHIN = 671,
     WITHOUT = 672,
     WORK = 673,
     WRAPPER = 674,
     WRITE = 675,
     XML_P = 676,
     XMLATTRIBUTES = 677,
     XMLCONCAT = 678,
     XMLELEMENT = 679,
     XMLEXISTS = 680,
     XMLFOREST = 681,
     XMLPARSE = 682,
     XMLPI = 683,
     XMLROOT = 684,
     XMLSERIALIZE = 685,
     YEAR_P = 686,
     YES_P = 687,
     ZONE = 688,
     NOT_LA = 689,
     NULLS_LA = 690,
     WITH_LA = 691,
     POSTFIXOP = 692,
     UMINUS = 693
   };
#endif
/* Tokens.  */
#define IDENT 258
#define FCONST 259
#define SCONST 260
#define BCONST 261
#define XCONST 262
#define Op 263
#define ICONST 264
#define PARAM 265
#define TYPECAST 266
#define DOT_DOT 267
#define COLON_EQUALS 268
#define EQUALS_GREATER 269
#define LESS_EQUALS 270
#define GREATER_EQUALS 271
#define NOT_EQUALS 272
#define ABORT_P 273
#define ABSOLUTE_P 274
#define ACCESS 275
#define ACTION 276
#define ADD_P 277
#define ADMIN 278
#define AFTER 279
#define AGGREGATE 280
#define ALL 281
#define ALSO 282
#define ALTER 283
#define ALWAYS 284
#define ANALYSE 285
#define ANALYZE 286
#define AND 287
#define ANY 288
#define ARRAY 289
#define AS 290
#define ASC 291
#define ASSERTION 292
#define ASSIGNMENT 293
#define ASYMMETRIC 294
#define AT 295
#define ATTRIBUTE 296
#define AUTHORIZATION 297
#define BACKWARD 298
#define BEFORE 299
#define BEGIN_P 300
#define BETWEEN 301
#define BIGINT 302
#define BINARY 303
#define BIT 304
#define BOOLEAN_P 305
#define BOTH 306
#define BY 307
#define CACHE 308
#define CALLED 309
#define CASCADE 310
#define CASCADED 311
#define CASE 312
#define CAST 313
#define CATALOG_P 314
#define CHAIN 315
#define CHAR_P 316
#define CHARACTER 317
#define CHARACTERISTICS 318
#define CHECK 319
#define CHECKPOINT 320
#define CLASS 321
#define CLOSE 322
#define CLUSTER 323
#define COALESCE 324
#define COLLATE 325
#define COLLATION 326
#define COLUMN 327
#define COMMENT 328
#define COMMENTS 329
#define COMMIT 330
#define COMMITTED 331
#define CONCURRENTLY 332
#define CONFIGURATION 333
#define CONFLICT 334
#define CONNECTION 335
#define CONSTRAINT 336
#define CONSTRAINTS 337
#define CONTENT_P 338
#define CONTINUE_P 339
#define CONVERSION_P 340
#define COPY 341
#define COST 342
#define CREATE 343
#define CROSS 344
#define CSV 345
#define CUBE 346
#define CURRENT_P 347
#define CURRENT_CATALOG 348
#define CURRENT_DATE 349
#define CURRENT_ROLE 350
#define CURRENT_SCHEMA 351
#define CURRENT_TIME 352
#define CURRENT_TIMESTAMP 353
#define CURRENT_USER 354
#define CURSOR 355
#define CYCLE 356
#define DATA_P 357
#define DATABASE 358
#define DAY_P 359
#define DEALLOCATE 360
#define DEC 361
#define DECIMAL_P 362
#define DECLARE 363
#define DEFAULT 364
#define DEFAULTS 365
#define DEFERRABLE 366
#define DEFERRED 367
#define DEFINER 368
#define DELETE_P 369
#define DELIMITER 370
#define DELIMITERS 371
#define DESC 372
#define DICTIONARY 373
#define DISABLE_P 374
#define DISCARD 375
#define DISTINCT 376
#define DO 377
#define DOCUMENT_P 378
#define DOMAIN_P 379
#define DOUBLE_P 380
#define DROP 381
#define EACH 382
#define ELSE 383
#define ENABLE_P 384
#define ENCODING 385
#define ENCRYPTED 386
#define END_P 387
#define ENUM_P 388
#define ESCAPE 389
#define EVENT 390
#define EXCEPT 391
#define EXCLUDE 392
#define EXCLUDING 393
#define EXCLUSIVE 394
#define EXECUTE 395
#define EXISTS 396
#define EXPLAIN 397
#define EXTENSION 398
#define EXTERNAL 399
#define EXTRACT 400
#define FALSE_P 401
#define FAMILY 402
#define FETCH 403
#define FILTER 404
#define FIRST_P 405
#define FLOAT_P 406
#define FOLLOWING 407
#define FOR 408
#define FORCE 409
#define FOREIGN 410
#define FORWARD 411
#define FREEZE 412
#define FROM 413
#define FULL 414
#define FUNCTION 415
#define FUNCTIONS 416
#define GLOBAL 417
#define GRANT 418
#define GRANTED 419
#define GREATEST 420
#define GROUP_P 421
#define GROUPING 422
#define HANDLER 423
#define HAVING 424
#define HEADER_P 425
#define HOLD 426
#define HOUR_P 427
#define IDENTITY_P 428
#define IF_P 429
#define ILIKE 430
#define IMMEDIATE 431
#define IMMUTABLE 432
#define IMPLICIT_P 433
#define IMPORT_P 434
#define IN_P 435
#define INCLUDING 436
#define INCREMENT 437
#define INDEX 438
#define INDEXES 439
#define INHERIT 440
#define INHERITS 441
#define INITIALLY 442
#define INLINE_P 443
#define INNER_P 444
#define INOUT 445
#define INPUT_P 446
#define INSENSITIVE 447
#define INSERT 448
#define INSTEAD 449
#define INT_P 450
#define INTEGER 451
#define INTERSECT 452
#define INTERVAL 453
#define INTO 454
#define INVOKER 455
#define IS 456
#define ISNULL 457
#define ISOLATION 458
#define JOIN 459
#define KEY 460
#define LABEL 461
#define LANGUAGE 462
#define LARGE_P 463
#define LAST_P 464
#define LATERAL_P 465
#define LEADING 466
#define LEAKPROOF 467
#define LEAST 468
#define LEFT 469
#define LEVEL 470
#define LIKE 471
#define LIMIT 472
#define LISTEN 473
#define LOAD 474
#define LOCAL 475
#define LOCALTIME 476
#define LOCALTIMESTAMP 477
#define LOCATION 478
#define LOCK_P 479
#define LOCKED 480
#define LOGGED 481
#define MAPPING 482
#define MATCH 483
#define MATERIALIZED 484
#define MAXVALUE 485
#define MINUTE_P 486
#define MINVALUE 487
#define MODE 488
#define MONTH_P 489
#define MOVE 490
#define NAME_P 491
#define NAMES 492
#define NATIONAL 493
#define NATURAL 494
#define NCHAR 495
#define NEXT 496
#define NO 497
#define NONE 498
#define NOT 499
#define NOTHING 500
#define NOTIFY 501
#define NOTNULL 502
#define NOWAIT 503
#define NULL_P 504
#define NULLIF 505
#define NULLS_P 506
#define NUMERIC 507
#define OBJECT_P 508
#define OF 509
#define OFF 510
#define OFFSET 511
#define OIDS 512
#define ON 513
#define ONLY 514
#define OPERATOR 515
#define OPTION 516
#define OPTIONS 517
#define OR 518
#define ORDER 519
#define ORDINALITY 520
#define OUT_P 521
#define OUTER_P 522
#define OVER 523
#define OVERLAPS 524
#define OVERLAY 525
#define OWNED 526
#define OWNER 527
#define PARSER 528
#define PARTIAL 529
#define PARTITION 530
#define PASSING 531
#define PASSWORD 532
#define PLACING 533
#define PLANS 534
#define POLICY 535
#define POSITION 536
#define PRECEDING 537
#define PRECISION 538
#define PRESERVE 539
#define PREPARE 540
#define PREPARED 541
#define PRIMARY 542
#define PRIOR 543
#define PRIVILEGES 544
#define PROCEDURAL 545
#define PROCEDURE 546
#define PROGRAM 547
#define QUOTE 548
#define RANGE 549
#define READ 550
#define REAL 551
#define REASSIGN 552
#define RECHECK 553
#define RECURSIVE 554
#define REF 555
#define REFERENCES 556
#define REFRESH 557
#define REINDEX 558
#define RELATIVE_P 559
#define RELEASE 560
#define RENAME 561
#define REPEATABLE 562
#define REPLACE 563
#define REPLICA 564
#define RESET 565
#define RESTART 566
#define RESTRICT 567
#define RETURNING 568
#define RETURNS 569
#define REVOKE 570
#define RIGHT 571
#define ROLE 572
#define ROLLBACK 573
#define ROLLUP 574
#define ROW 575
#define ROWS 576
#define RULE 577
#define SAVEPOINT 578
#define SCHEMA 579
#define SCROLL 580
#define SEARCH 581
#define SECOND_P 582
#define SECURITY 583
#define SELECT 584
#define SEQUENCE 585
#define SEQUENCES 586
#define SERIALIZABLE 587
#define SERVER 588
#define SESSION 589
#define SESSION_USER 590
#define SET 591
#define SETS 592
#define SETOF 593
#define SHARE 594
#define SHOW 595
#define SIMILAR 596
#define SIMPLE 597
#define SKIP 598
#define SMALLINT 599
#define SNAPSHOT 600
#define SOME 601
#define SQL_P 602
#define STABLE 603
#define STANDALONE_P 604
#define START 605
#define STATEMENT 606
#define STATISTICS 607
#define STDIN 608
#define STDOUT 609
#define STORAGE 610
#define STRICT_P 611
#define STRIP_P 612
#define SUBSTRING 613
#define SYMMETRIC 614
#define SYSID 615
#define SYSTEM_P 616
#define TABLE 617
#define TABLES 618
#define TABLESAMPLE 619
#define TABLESPACE 620
#define TEMP 621
#define TEMPLATE 622
#define TEMPORARY 623
#define TEXT_P 624
#define THEN 625
#define TIME 626
#define TIMESTAMP 627
#define TO 628
#define TRAILING 629
#define TRANSACTION 630
#define TRANSFORM 631
#define TREAT 632
#define TRIGGER 633
#define TRIM 634
#define TRUE_P 635
#define TRUNCATE 636
#define TRUSTED 637
#define TYPE_P 638
#define TYPES_P 639
#define UNBOUNDED 640
#define UNCOMMITTED 641
#define UNENCRYPTED 642
#define UNION 643
#define UNIQUE 644
#define UNKNOWN 645
#define UNLISTEN 646
#define UNLOGGED 647
#define UNTIL 648
#define UPDATE 649
#define USER 650
#define USING 651
#define VACUUM 652
#define VALID 653
#define VALIDATE 654
#define VALIDATOR 655
#define VALUE_P 656
#define VALUES 657
#define VARCHAR 658
#define VARIADIC 659
#define VARYING 660
#define VERBOSE 661
#define VERSION_P 662
#define VIEW 663
#define VIEWS 664
#define VOLATILE 665
#define WHEN 666
#define WHERE 667
#define WHITESPACE_P 668
#define WINDOW 669
#define WITH 670
#define WITHIN 671
#define WITHOUT 672
#define WORK 673
#define WRAPPER 674
#define WRITE 675
#define XML_P 676
#define XMLATTRIBUTES 677
#define XMLCONCAT 678
#define XMLELEMENT 679
#define XMLEXISTS 680
#define XMLFOREST 681
#define XMLPARSE 682
#define XMLPI 683
#define XMLROOT 684
#define XMLSERIALIZE 685
#define YEAR_P 686
#define YES_P 687
#define ZONE 688
#define NOT_LA 689
#define NULLS_LA 690
#define WITH_LA 691
#define POSTFIXOP 692
#define UMINUS 693




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 194 "gram.y"
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	FuncWithArgs		*funwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	InferClause			*infer;
	OnConflictClause	*onconflict;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	struct ImportQual	*importqual;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
}
/* Line 1489 of yacc.c.  */
#line 965 "src_backend_parser_gram.h"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


