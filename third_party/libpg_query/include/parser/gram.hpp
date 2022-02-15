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
     LAMBDA_ARROW = 270,
     LESS_EQUALS = 271,
     GREATER_EQUALS = 272,
     NOT_EQUALS = 273,
     ABORT_P = 274,
     ABSOLUTE_P = 275,
     ACCESS = 276,
     ACTION = 277,
     ADD_P = 278,
     ADMIN = 279,
     AFTER = 280,
     AGGREGATE = 281,
     ALL = 282,
     ALSO = 283,
     ALTER = 284,
     ALWAYS = 285,
     ANALYSE = 286,
     ANALYZE = 287,
     AND = 288,
     ANY = 289,
     ARRAY = 290,
     AS = 291,
     ASC_P = 292,
     ASSERTION = 293,
     ASSIGNMENT = 294,
     ASYMMETRIC = 295,
     AT = 296,
     ATTACH = 297,
     ATTRIBUTE = 298,
     AUTHORIZATION = 299,
     BACKWARD = 300,
     BEFORE = 301,
     BEGIN_P = 302,
     BETWEEN = 303,
     BIGINT = 304,
     BINARY = 305,
     BIT = 306,
     BOOLEAN_P = 307,
     BOTH = 308,
     BY = 309,
     CACHE = 310,
     CALL_P = 311,
     CALLED = 312,
     CASCADE = 313,
     CASCADED = 314,
     CASE = 315,
     CAST = 316,
     CATALOG_P = 317,
     CHAIN = 318,
     CHAR_P = 319,
     CHARACTER = 320,
     CHARACTERISTICS = 321,
     CHECK_P = 322,
     CHECKPOINT = 323,
     CLASS = 324,
     CLOSE = 325,
     CLUSTER = 326,
     COALESCE = 327,
     COLLATE = 328,
     COLLATION = 329,
     COLUMN = 330,
     COLUMNS = 331,
     COMMENT = 332,
     COMMENTS = 333,
     COMMIT = 334,
     COMMITTED = 335,
     COMPRESSION = 336,
     CONCURRENTLY = 337,
     CONFIGURATION = 338,
     CONFLICT = 339,
     CONNECTION = 340,
     CONSTRAINT = 341,
     CONSTRAINTS = 342,
     CONTENT_P = 343,
     CONTINUE_P = 344,
     CONVERSION_P = 345,
     COPY = 346,
     COST = 347,
     CREATE_P = 348,
     CROSS = 349,
     CSV = 350,
     CUBE = 351,
     CURRENT_P = 352,
     CURRENT_CATALOG = 353,
     CURRENT_DATE = 354,
     CURRENT_ROLE = 355,
     CURRENT_SCHEMA = 356,
     CURRENT_TIME = 357,
     CURRENT_TIMESTAMP = 358,
     CURRENT_USER = 359,
     CURSOR = 360,
     CYCLE = 361,
     DATA_P = 362,
     DATABASE = 363,
     DAY_P = 364,
     DAYS_P = 365,
     DEALLOCATE = 366,
     DEC = 367,
     DECIMAL_P = 368,
     DECLARE = 369,
     DEFAULT = 370,
     DEFAULTS = 371,
     DEFERRABLE = 372,
     DEFERRED = 373,
     DEFINER = 374,
     DELETE_P = 375,
     DELIMITER = 376,
     DELIMITERS = 377,
     DEPENDS = 378,
     DESC_P = 379,
     DESCRIBE = 380,
     DETACH = 381,
     DICTIONARY = 382,
     DISABLE_P = 383,
     DISCARD = 384,
     DISTINCT = 385,
     DO = 386,
     DOCUMENT_P = 387,
     DOMAIN_P = 388,
     DOUBLE_P = 389,
     DROP = 390,
     EACH = 391,
     ELSE = 392,
     ENABLE_P = 393,
     ENCODING = 394,
     ENCRYPTED = 395,
     END_P = 396,
     ENUM_P = 397,
     ESCAPE = 398,
     EVENT = 399,
     EXCEPT = 400,
     EXCLUDE = 401,
     EXCLUDING = 402,
     EXCLUSIVE = 403,
     EXECUTE = 404,
     EXISTS = 405,
     EXPLAIN = 406,
     EXPORT_P = 407,
     EXPORT_STATE = 408,
     EXTENSION = 409,
     EXTERNAL = 410,
     EXTRACT = 411,
     FALSE_P = 412,
     FAMILY = 413,
     FETCH = 414,
     FILTER = 415,
     FIRST_P = 416,
     FLOAT_P = 417,
     FOLLOWING = 418,
     FOR = 419,
     FORCE = 420,
     FOREIGN = 421,
     FORWARD = 422,
     FREEZE = 423,
     FROM = 424,
     FULL = 425,
     FUNCTION = 426,
     FUNCTIONS = 427,
     GENERATED = 428,
     GLOB = 429,
     GLOBAL = 430,
     GRANT = 431,
     GRANTED = 432,
     GROUP_P = 433,
     GROUPING = 434,
     GROUPING_ID = 435,
     HANDLER = 436,
     HAVING = 437,
     HEADER_P = 438,
     HOLD = 439,
     HOUR_P = 440,
     HOURS_P = 441,
     IDENTITY_P = 442,
     IF_P = 443,
     IGNORE_P = 444,
     ILIKE = 445,
     IMMEDIATE = 446,
     IMMUTABLE = 447,
     IMPLICIT_P = 448,
     IMPORT_P = 449,
     IN_P = 450,
     INCLUDING = 451,
     INCREMENT = 452,
     INDEX = 453,
     INDEXES = 454,
     INHERIT = 455,
     INHERITS = 456,
     INITIALLY = 457,
     INLINE_P = 458,
     INNER_P = 459,
     INOUT = 460,
     INPUT_P = 461,
     INSENSITIVE = 462,
     INSERT = 463,
     INSTALL = 464,
     INSTEAD = 465,
     INT_P = 466,
     INTEGER = 467,
     INTERSECT = 468,
     INTERVAL = 469,
     INTO = 470,
     INVOKER = 471,
     IS = 472,
     ISNULL = 473,
     ISOLATION = 474,
     JOIN = 475,
     KEY = 476,
     LABEL = 477,
     LANGUAGE = 478,
     LARGE_P = 479,
     LAST_P = 480,
     LATERAL_P = 481,
     LEADING = 482,
     LEAKPROOF = 483,
     LEFT = 484,
     LEVEL = 485,
     LIKE = 486,
     LIMIT = 487,
     LISTEN = 488,
     LOAD = 489,
     LOCAL = 490,
     LOCALTIME = 491,
     LOCALTIMESTAMP = 492,
     LOCATION = 493,
     LOCK_P = 494,
     LOCKED = 495,
     LOGGED = 496,
     MACRO = 497,
     MAP = 498,
     MAPPING = 499,
     MATCH = 500,
     MATERIALIZED = 501,
     MAXVALUE = 502,
     METHOD = 503,
     MICROSECOND_P = 504,
     MICROSECONDS_P = 505,
     MILLISECOND_P = 506,
     MILLISECONDS_P = 507,
     MINUTE_P = 508,
     MINUTES_P = 509,
     MINVALUE = 510,
     MODE = 511,
     MONTH_P = 512,
     MONTHS_P = 513,
     MOVE = 514,
     NAME_P = 515,
     NAMES = 516,
     NATIONAL = 517,
     NATURAL = 518,
     NCHAR = 519,
     NEW = 520,
     NEXT = 521,
     NO = 522,
     NONE = 523,
     NOT = 524,
     NOTHING = 525,
     NOTIFY = 526,
     NOTNULL = 527,
     NOWAIT = 528,
     NULL_P = 529,
     NULLIF = 530,
     NULLS_P = 531,
     NUMERIC = 532,
     OBJECT_P = 533,
     OF = 534,
     OFF = 535,
     OFFSET = 536,
     OIDS = 537,
     OLD = 538,
     ON = 539,
     ONLY = 540,
     OPERATOR = 541,
     OPTION = 542,
     OPTIONS = 543,
     OR = 544,
     ORDER = 545,
     ORDINALITY = 546,
     OUT_P = 547,
     OUTER_P = 548,
     OVER = 549,
     OVERLAPS = 550,
     OVERLAY = 551,
     OVERRIDING = 552,
     OWNED = 553,
     OWNER = 554,
     PARALLEL = 555,
     PARSER = 556,
     PARTIAL = 557,
     PARTITION = 558,
     PASSING = 559,
     PASSWORD = 560,
     PERCENT = 561,
     PLACING = 562,
     PLANS = 563,
     POLICY = 564,
     POSITION = 565,
     PRAGMA_P = 566,
     PRECEDING = 567,
     PRECISION = 568,
     PREPARE = 569,
     PREPARED = 570,
     PRESERVE = 571,
     PRIMARY = 572,
     PRIOR = 573,
     PRIVILEGES = 574,
     PROCEDURAL = 575,
     PROCEDURE = 576,
     PROGRAM = 577,
     PUBLICATION = 578,
     QUALIFY = 579,
     QUOTE = 580,
     RANGE = 581,
     READ_P = 582,
     REAL = 583,
     REASSIGN = 584,
     RECHECK = 585,
     RECURSIVE = 586,
     REF = 587,
     REFERENCES = 588,
     REFERENCING = 589,
     REFRESH = 590,
     REINDEX = 591,
     RELATIVE_P = 592,
     RELEASE = 593,
     RENAME = 594,
     REPEATABLE = 595,
     REPLACE = 596,
     REPLICA = 597,
     RESET = 598,
     RESPECT_P = 599,
     RESTART = 600,
     RESTRICT = 601,
     RETURNING = 602,
     RETURNS = 603,
     REVOKE = 604,
     RIGHT = 605,
     ROLE = 606,
     ROLLBACK = 607,
     ROLLUP = 608,
     ROW = 609,
     ROWS = 610,
     RULE = 611,
     SAMPLE = 612,
     SAVEPOINT = 613,
     SCHEMA = 614,
     SCHEMAS = 615,
     SCROLL = 616,
     SEARCH = 617,
     SECOND_P = 618,
     SECONDS_P = 619,
     SECURITY = 620,
     SELECT = 621,
     SEQUENCE = 622,
     SEQUENCES = 623,
     SERIALIZABLE = 624,
     SERVER = 625,
     SESSION = 626,
     SESSION_USER = 627,
     SET = 628,
     SETOF = 629,
     SETS = 630,
     SHARE = 631,
     SHOW = 632,
     SIMILAR = 633,
     SIMPLE = 634,
     SKIP = 635,
     SMALLINT = 636,
     SNAPSHOT = 637,
     SOME = 638,
     SQL_P = 639,
     STABLE = 640,
     STANDALONE_P = 641,
     START = 642,
     STATEMENT = 643,
     STATISTICS = 644,
     STDIN = 645,
     STDOUT = 646,
     STORAGE = 647,
     STRICT_P = 648,
     STRIP_P = 649,
     STRUCT = 650,
     SUBSCRIPTION = 651,
     SUBSTRING = 652,
     SUMMARIZE = 653,
     SYMMETRIC = 654,
     SYSID = 655,
     SYSTEM_P = 656,
     TABLE = 657,
     TABLES = 658,
     TABLESAMPLE = 659,
     TABLESPACE = 660,
     TEMP = 661,
     TEMPLATE = 662,
     TEMPORARY = 663,
     TEXT_P = 664,
     THEN = 665,
     TIME = 666,
     TIMESTAMP = 667,
     TO = 668,
     TRAILING = 669,
     TRANSACTION = 670,
     TRANSFORM = 671,
     TREAT = 672,
     TRIGGER = 673,
     TRIM = 674,
     TRUE_P = 675,
     TRUNCATE = 676,
     TRUSTED = 677,
     TRY_CAST = 678,
     TYPE_P = 679,
     TYPES_P = 680,
     UNBOUNDED = 681,
     UNCOMMITTED = 682,
     UNENCRYPTED = 683,
     UNION = 684,
     UNIQUE = 685,
     UNKNOWN = 686,
     UNLISTEN = 687,
     UNLOGGED = 688,
     UNTIL = 689,
     UPDATE = 690,
     USER = 691,
     USING = 692,
     VACUUM = 693,
     VALID = 694,
     VALIDATE = 695,
     VALIDATOR = 696,
     VALUE_P = 697,
     VALUES = 698,
     VARCHAR = 699,
     VARIADIC = 700,
     VARYING = 701,
     VERBOSE = 702,
     VERSION_P = 703,
     VIEW = 704,
     VIEWS = 705,
     VOLATILE = 706,
     WHEN = 707,
     WHERE = 708,
     WHITESPACE_P = 709,
     WINDOW = 710,
     WITH = 711,
     WITHIN = 712,
     WITHOUT = 713,
     WORK = 714,
     WRAPPER = 715,
     WRITE_P = 716,
     XML_P = 717,
     XMLATTRIBUTES = 718,
     XMLCONCAT = 719,
     XMLELEMENT = 720,
     XMLEXISTS = 721,
     XMLFOREST = 722,
     XMLNAMESPACES = 723,
     XMLPARSE = 724,
     XMLPI = 725,
     XMLROOT = 726,
     XMLSERIALIZE = 727,
     XMLTABLE = 728,
     YEAR_P = 729,
     YEARS_P = 730,
     YES_P = 731,
     ZONE = 732,
     NOT_LA = 733,
     NULLS_LA = 734,
     WITH_LA = 735,
     POSTFIXOP = 736,
     UMINUS = 737
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
#define LAMBDA_ARROW 270
#define LESS_EQUALS 271
#define GREATER_EQUALS 272
#define NOT_EQUALS 273
#define ABORT_P 274
#define ABSOLUTE_P 275
#define ACCESS 276
#define ACTION 277
#define ADD_P 278
#define ADMIN 279
#define AFTER 280
#define AGGREGATE 281
#define ALL 282
#define ALSO 283
#define ALTER 284
#define ALWAYS 285
#define ANALYSE 286
#define ANALYZE 287
#define AND 288
#define ANY 289
#define ARRAY 290
#define AS 291
#define ASC_P 292
#define ASSERTION 293
#define ASSIGNMENT 294
#define ASYMMETRIC 295
#define AT 296
#define ATTACH 297
#define ATTRIBUTE 298
#define AUTHORIZATION 299
#define BACKWARD 300
#define BEFORE 301
#define BEGIN_P 302
#define BETWEEN 303
#define BIGINT 304
#define BINARY 305
#define BIT 306
#define BOOLEAN_P 307
#define BOTH 308
#define BY 309
#define CACHE 310
#define CALL_P 311
#define CALLED 312
#define CASCADE 313
#define CASCADED 314
#define CASE 315
#define CAST 316
#define CATALOG_P 317
#define CHAIN 318
#define CHAR_P 319
#define CHARACTER 320
#define CHARACTERISTICS 321
#define CHECK_P 322
#define CHECKPOINT 323
#define CLASS 324
#define CLOSE 325
#define CLUSTER 326
#define COALESCE 327
#define COLLATE 328
#define COLLATION 329
#define COLUMN 330
#define COLUMNS 331
#define COMMENT 332
#define COMMENTS 333
#define COMMIT 334
#define COMMITTED 335
#define COMPRESSION 336
#define CONCURRENTLY 337
#define CONFIGURATION 338
#define CONFLICT 339
#define CONNECTION 340
#define CONSTRAINT 341
#define CONSTRAINTS 342
#define CONTENT_P 343
#define CONTINUE_P 344
#define CONVERSION_P 345
#define COPY 346
#define COST 347
#define CREATE_P 348
#define CROSS 349
#define CSV 350
#define CUBE 351
#define CURRENT_P 352
#define CURRENT_CATALOG 353
#define CURRENT_DATE 354
#define CURRENT_ROLE 355
#define CURRENT_SCHEMA 356
#define CURRENT_TIME 357
#define CURRENT_TIMESTAMP 358
#define CURRENT_USER 359
#define CURSOR 360
#define CYCLE 361
#define DATA_P 362
#define DATABASE 363
#define DAY_P 364
#define DAYS_P 365
#define DEALLOCATE 366
#define DEC 367
#define DECIMAL_P 368
#define DECLARE 369
#define DEFAULT 370
#define DEFAULTS 371
#define DEFERRABLE 372
#define DEFERRED 373
#define DEFINER 374
#define DELETE_P 375
#define DELIMITER 376
#define DELIMITERS 377
#define DEPENDS 378
#define DESC_P 379
#define DESCRIBE 380
#define DETACH 381
#define DICTIONARY 382
#define DISABLE_P 383
#define DISCARD 384
#define DISTINCT 385
#define DO 386
#define DOCUMENT_P 387
#define DOMAIN_P 388
#define DOUBLE_P 389
#define DROP 390
#define EACH 391
#define ELSE 392
#define ENABLE_P 393
#define ENCODING 394
#define ENCRYPTED 395
#define END_P 396
#define ENUM_P 397
#define ESCAPE 398
#define EVENT 399
#define EXCEPT 400
#define EXCLUDE 401
#define EXCLUDING 402
#define EXCLUSIVE 403
#define EXECUTE 404
#define EXISTS 405
#define EXPLAIN 406
#define EXPORT_P 407
#define EXPORT_STATE 408
#define EXTENSION 409
#define EXTERNAL 410
#define EXTRACT 411
#define FALSE_P 412
#define FAMILY 413
#define FETCH 414
#define FILTER 415
#define FIRST_P 416
#define FLOAT_P 417
#define FOLLOWING 418
#define FOR 419
#define FORCE 420
#define FOREIGN 421
#define FORWARD 422
#define FREEZE 423
#define FROM 424
#define FULL 425
#define FUNCTION 426
#define FUNCTIONS 427
#define GENERATED 428
#define GLOB 429
#define GLOBAL 430
#define GRANT 431
#define GRANTED 432
#define GROUP_P 433
#define GROUPING 434
#define GROUPING_ID 435
#define HANDLER 436
#define HAVING 437
#define HEADER_P 438
#define HOLD 439
#define HOUR_P 440
#define HOURS_P 441
#define IDENTITY_P 442
#define IF_P 443
#define IGNORE_P 444
#define ILIKE 445
#define IMMEDIATE 446
#define IMMUTABLE 447
#define IMPLICIT_P 448
#define IMPORT_P 449
#define IN_P 450
#define INCLUDING 451
#define INCREMENT 452
#define INDEX 453
#define INDEXES 454
#define INHERIT 455
#define INHERITS 456
#define INITIALLY 457
#define INLINE_P 458
#define INNER_P 459
#define INOUT 460
#define INPUT_P 461
#define INSENSITIVE 462
#define INSERT 463
#define INSTALL 464
#define INSTEAD 465
#define INT_P 466
#define INTEGER 467
#define INTERSECT 468
#define INTERVAL 469
#define INTO 470
#define INVOKER 471
#define IS 472
#define ISNULL 473
#define ISOLATION 474
#define JOIN 475
#define KEY 476
#define LABEL 477
#define LANGUAGE 478
#define LARGE_P 479
#define LAST_P 480
#define LATERAL_P 481
#define LEADING 482
#define LEAKPROOF 483
#define LEFT 484
#define LEVEL 485
#define LIKE 486
#define LIMIT 487
#define LISTEN 488
#define LOAD 489
#define LOCAL 490
#define LOCALTIME 491
#define LOCALTIMESTAMP 492
#define LOCATION 493
#define LOCK_P 494
#define LOCKED 495
#define LOGGED 496
#define MACRO 497
#define MAP 498
#define MAPPING 499
#define MATCH 500
#define MATERIALIZED 501
#define MAXVALUE 502
#define METHOD 503
#define MICROSECOND_P 504
#define MICROSECONDS_P 505
#define MILLISECOND_P 506
#define MILLISECONDS_P 507
#define MINUTE_P 508
#define MINUTES_P 509
#define MINVALUE 510
#define MODE 511
#define MONTH_P 512
#define MONTHS_P 513
#define MOVE 514
#define NAME_P 515
#define NAMES 516
#define NATIONAL 517
#define NATURAL 518
#define NCHAR 519
#define NEW 520
#define NEXT 521
#define NO 522
#define NONE 523
#define NOT 524
#define NOTHING 525
#define NOTIFY 526
#define NOTNULL 527
#define NOWAIT 528
#define NULL_P 529
#define NULLIF 530
#define NULLS_P 531
#define NUMERIC 532
#define OBJECT_P 533
#define OF 534
#define OFF 535
#define OFFSET 536
#define OIDS 537
#define OLD 538
#define ON 539
#define ONLY 540
#define OPERATOR 541
#define OPTION 542
#define OPTIONS 543
#define OR 544
#define ORDER 545
#define ORDINALITY 546
#define OUT_P 547
#define OUTER_P 548
#define OVER 549
#define OVERLAPS 550
#define OVERLAY 551
#define OVERRIDING 552
#define OWNED 553
#define OWNER 554
#define PARALLEL 555
#define PARSER 556
#define PARTIAL 557
#define PARTITION 558
#define PASSING 559
#define PASSWORD 560
#define PERCENT 561
#define PLACING 562
#define PLANS 563
#define POLICY 564
#define POSITION 565
#define PRAGMA_P 566
#define PRECEDING 567
#define PRECISION 568
#define PREPARE 569
#define PREPARED 570
#define PRESERVE 571
#define PRIMARY 572
#define PRIOR 573
#define PRIVILEGES 574
#define PROCEDURAL 575
#define PROCEDURE 576
#define PROGRAM 577
#define PUBLICATION 578
#define QUALIFY 579
#define QUOTE 580
#define RANGE 581
#define READ_P 582
#define REAL 583
#define REASSIGN 584
#define RECHECK 585
#define RECURSIVE 586
#define REF 587
#define REFERENCES 588
#define REFERENCING 589
#define REFRESH 590
#define REINDEX 591
#define RELATIVE_P 592
#define RELEASE 593
#define RENAME 594
#define REPEATABLE 595
#define REPLACE 596
#define REPLICA 597
#define RESET 598
#define RESPECT_P 599
#define RESTART 600
#define RESTRICT 601
#define RETURNING 602
#define RETURNS 603
#define REVOKE 604
#define RIGHT 605
#define ROLE 606
#define ROLLBACK 607
#define ROLLUP 608
#define ROW 609
#define ROWS 610
#define RULE 611
#define SAMPLE 612
#define SAVEPOINT 613
#define SCHEMA 614
#define SCHEMAS 615
#define SCROLL 616
#define SEARCH 617
#define SECOND_P 618
#define SECONDS_P 619
#define SECURITY 620
#define SELECT 621
#define SEQUENCE 622
#define SEQUENCES 623
#define SERIALIZABLE 624
#define SERVER 625
#define SESSION 626
#define SESSION_USER 627
#define SET 628
#define SETOF 629
#define SETS 630
#define SHARE 631
#define SHOW 632
#define SIMILAR 633
#define SIMPLE 634
#define SKIP 635
#define SMALLINT 636
#define SNAPSHOT 637
#define SOME 638
#define SQL_P 639
#define STABLE 640
#define STANDALONE_P 641
#define START 642
#define STATEMENT 643
#define STATISTICS 644
#define STDIN 645
#define STDOUT 646
#define STORAGE 647
#define STRICT_P 648
#define STRIP_P 649
#define STRUCT 650
#define SUBSCRIPTION 651
#define SUBSTRING 652
#define SUMMARIZE 653
#define SYMMETRIC 654
#define SYSID 655
#define SYSTEM_P 656
#define TABLE 657
#define TABLES 658
#define TABLESAMPLE 659
#define TABLESPACE 660
#define TEMP 661
#define TEMPLATE 662
#define TEMPORARY 663
#define TEXT_P 664
#define THEN 665
#define TIME 666
#define TIMESTAMP 667
#define TO 668
#define TRAILING 669
#define TRANSACTION 670
#define TRANSFORM 671
#define TREAT 672
#define TRIGGER 673
#define TRIM 674
#define TRUE_P 675
#define TRUNCATE 676
#define TRUSTED 677
#define TRY_CAST 678
#define TYPE_P 679
#define TYPES_P 680
#define UNBOUNDED 681
#define UNCOMMITTED 682
#define UNENCRYPTED 683
#define UNION 684
#define UNIQUE 685
#define UNKNOWN 686
#define UNLISTEN 687
#define UNLOGGED 688
#define UNTIL 689
#define UPDATE 690
#define USER 691
#define USING 692
#define VACUUM 693
#define VALID 694
#define VALIDATE 695
#define VALIDATOR 696
#define VALUE_P 697
#define VALUES 698
#define VARCHAR 699
#define VARIADIC 700
#define VARYING 701
#define VERBOSE 702
#define VERSION_P 703
#define VIEW 704
#define VIEWS 705
#define VOLATILE 706
#define WHEN 707
#define WHERE 708
#define WHITESPACE_P 709
#define WINDOW 710
#define WITH 711
#define WITHIN 712
#define WITHOUT 713
#define WORK 714
#define WRAPPER 715
#define WRITE_P 716
#define XML_P 717
#define XMLATTRIBUTES 718
#define XMLCONCAT 719
#define XMLELEMENT 720
#define XMLEXISTS 721
#define XMLFOREST 722
#define XMLNAMESPACES 723
#define XMLPARSE 724
#define XMLPI 725
#define XMLROOT 726
#define XMLSERIALIZE 727
#define XMLTABLE 728
#define YEAR_P 729
#define YEARS_P 730
#define YES_P 731
#define ZONE 732
#define NOT_LA 733
#define NULLS_LA 734
#define WITH_LA 735
#define POSTFIXOP 736
#define UMINUS 737




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 14 "third_party/libpg_query/grammar/grammar.y"
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;
	const char          *conststr;

	char				chr;
	bool				boolean;
	PGJoinType			jtype;
	PGDropBehavior		dbehavior;
	PGOnCommitAction		oncommit;
	PGOnCreateConflict		oncreateconflict;
	PGList				*list;
	PGNode				*node;
	PGValue				*value;
	PGObjectType			objtype;
	PGTypeName			*typnam;
	PGObjectWithArgs		*objwithargs;
	PGDefElem				*defelt;
	PGSortBy				*sortby;
	PGWindowDef			*windef;
	PGJoinExpr			*jexpr;
	PGIndexElem			*ielem;
	PGAlias				*alias;
	PGRangeVar			*range;
	PGIntoClause			*into;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
}
/* Line 1529 of yacc.c.  */
#line 1057 "third_party/libpg_query/grammar/grammar_out.hpp"
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


