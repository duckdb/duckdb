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
     POWER_OF = 271,
     LESS_EQUALS = 272,
     GREATER_EQUALS = 273,
     NOT_EQUALS = 274,
     ABORT_P = 275,
     ABSOLUTE_P = 276,
     ACCESS = 277,
     ACTION = 278,
     ADD_P = 279,
     ADMIN = 280,
     AFTER = 281,
     AGGREGATE = 282,
     ALL = 283,
     ALSO = 284,
     ALTER = 285,
     ALWAYS = 286,
     ANALYSE = 287,
     ANALYZE = 288,
     AND = 289,
     ANY = 290,
     ARRAY = 291,
     AS = 292,
     ASC_P = 293,
     ASSERTION = 294,
     ASSIGNMENT = 295,
     ASYMMETRIC = 296,
     AT = 297,
     ATTACH = 298,
     ATTRIBUTE = 299,
     AUTHORIZATION = 300,
     BACKWARD = 301,
     BEFORE = 302,
     BEGIN_P = 303,
     BETWEEN = 304,
     BIGINT = 305,
     BINARY = 306,
     BIT = 307,
     BOOLEAN_P = 308,
     BOTH = 309,
     BY = 310,
     CACHE = 311,
     CALL_P = 312,
     CALLED = 313,
     CASCADE = 314,
     CASCADED = 315,
     CASE = 316,
     CAST = 317,
     CATALOG_P = 318,
     CHAIN = 319,
     CHAR_P = 320,
     CHARACTER = 321,
     CHARACTERISTICS = 322,
     CHECK_P = 323,
     CHECKPOINT = 324,
     CLASS = 325,
     CLOSE = 326,
     CLUSTER = 327,
     COALESCE = 328,
     COLLATE = 329,
     COLLATION = 330,
     COLUMN = 331,
     COLUMNS = 332,
     COMMENT = 333,
     COMMENTS = 334,
     COMMIT = 335,
     COMMITTED = 336,
     COMPRESSION = 337,
     CONCURRENTLY = 338,
     CONFIGURATION = 339,
     CONFLICT = 340,
     CONNECTION = 341,
     CONSTRAINT = 342,
     CONSTRAINTS = 343,
     CONTENT_P = 344,
     CONTINUE_P = 345,
     CONVERSION_P = 346,
     COPY = 347,
     COST = 348,
     CREATE_P = 349,
     CROSS = 350,
     CSV = 351,
     CUBE = 352,
     CURRENT_P = 353,
     CURRENT_CATALOG = 354,
     CURRENT_DATE = 355,
     CURRENT_ROLE = 356,
     CURRENT_SCHEMA = 357,
     CURRENT_TIME = 358,
     CURRENT_TIMESTAMP = 359,
     CURRENT_USER = 360,
     CURSOR = 361,
     CYCLE = 362,
     DATA_P = 363,
     DATABASE = 364,
     DAY_P = 365,
     DAYS_P = 366,
     DEALLOCATE = 367,
     DEC = 368,
     DECIMAL_P = 369,
     DECLARE = 370,
     DEFAULT = 371,
     DEFAULTS = 372,
     DEFERRABLE = 373,
     DEFERRED = 374,
     DEFINER = 375,
     DELETE_P = 376,
     DELIMITER = 377,
     DELIMITERS = 378,
     DEPENDS = 379,
     DESC_P = 380,
     DESCRIBE = 381,
     DETACH = 382,
     DICTIONARY = 383,
     DISABLE_P = 384,
     DISCARD = 385,
     DISTINCT = 386,
     DO = 387,
     DOCUMENT_P = 388,
     DOMAIN_P = 389,
     DOUBLE_P = 390,
     DROP = 391,
     EACH = 392,
     ELSE = 393,
     ENABLE_P = 394,
     ENCODING = 395,
     ENCRYPTED = 396,
     END_P = 397,
     ENUM_P = 398,
     ESCAPE = 399,
     EVENT = 400,
     EXCEPT = 401,
     EXCLUDE = 402,
     EXCLUDING = 403,
     EXCLUSIVE = 404,
     EXECUTE = 405,
     EXISTS = 406,
     EXPLAIN = 407,
     EXPORT_P = 408,
     EXPORT_STATE = 409,
     EXTENSION = 410,
     EXTERNAL = 411,
     EXTRACT = 412,
     FALSE_P = 413,
     FAMILY = 414,
     FETCH = 415,
     FILTER = 416,
     FIRST_P = 417,
     FLOAT_P = 418,
     FOLLOWING = 419,
     FOR = 420,
     FORCE = 421,
     FOREIGN = 422,
     FORWARD = 423,
     FREEZE = 424,
     FROM = 425,
     FULL = 426,
     FUNCTION = 427,
     FUNCTIONS = 428,
     GENERATED = 429,
     GLOB = 430,
     GLOBAL = 431,
     GRANT = 432,
     GRANTED = 433,
     GROUP_P = 434,
     GROUPING = 435,
     GROUPING_ID = 436,
     HANDLER = 437,
     HAVING = 438,
     HEADER_P = 439,
     HOLD = 440,
     HOUR_P = 441,
     HOURS_P = 442,
     IDENTITY_P = 443,
     IF_P = 444,
     IGNORE_P = 445,
     ILIKE = 446,
     IMMEDIATE = 447,
     IMMUTABLE = 448,
     IMPLICIT_P = 449,
     IMPORT_P = 450,
     IN_P = 451,
     INCLUDING = 452,
     INCREMENT = 453,
     INDEX = 454,
     INDEXES = 455,
     INHERIT = 456,
     INHERITS = 457,
     INITIALLY = 458,
     INLINE_P = 459,
     INNER_P = 460,
     INOUT = 461,
     INPUT_P = 462,
     INSENSITIVE = 463,
     INSERT = 464,
     INSTALL = 465,
     INSTEAD = 466,
     INT_P = 467,
     INTEGER = 468,
     INTERSECT = 469,
     INTERVAL = 470,
     INTO = 471,
     INVOKER = 472,
     IS = 473,
     ISNULL = 474,
     ISOLATION = 475,
     JOIN = 476,
     JSON = 477,
     KEY = 478,
     LABEL = 479,
     LANGUAGE = 480,
     LARGE_P = 481,
     LAST_P = 482,
     LATERAL_P = 483,
     LEADING = 484,
     LEAKPROOF = 485,
     LEFT = 486,
     LEVEL = 487,
     LIKE = 488,
     LIMIT = 489,
     LISTEN = 490,
     LOAD = 491,
     LOCAL = 492,
     LOCALTIME = 493,
     LOCALTIMESTAMP = 494,
     LOCATION = 495,
     LOCK_P = 496,
     LOCKED = 497,
     LOGGED = 498,
     MACRO = 499,
     MAP = 500,
     MAPPING = 501,
     MATCH = 502,
     MATERIALIZED = 503,
     MAXVALUE = 504,
     METHOD = 505,
     MICROSECOND_P = 506,
     MICROSECONDS_P = 507,
     MILLISECOND_P = 508,
     MILLISECONDS_P = 509,
     MINUTE_P = 510,
     MINUTES_P = 511,
     MINVALUE = 512,
     MODE = 513,
     MONTH_P = 514,
     MONTHS_P = 515,
     MOVE = 516,
     NAME_P = 517,
     NAMES = 518,
     NATIONAL = 519,
     NATURAL = 520,
     NCHAR = 521,
     NEW = 522,
     NEXT = 523,
     NO = 524,
     NONE = 525,
     NOT = 526,
     NOTHING = 527,
     NOTIFY = 528,
     NOTNULL = 529,
     NOWAIT = 530,
     NULL_P = 531,
     NULLIF = 532,
     NULLS_P = 533,
     NUMERIC = 534,
     OBJECT_P = 535,
     OF = 536,
     OFF = 537,
     OFFSET = 538,
     OIDS = 539,
     OLD = 540,
     ON = 541,
     ONLY = 542,
     OPERATOR = 543,
     OPTION = 544,
     OPTIONS = 545,
     OR = 546,
     ORDER = 547,
     ORDINALITY = 548,
     OUT_P = 549,
     OUTER_P = 550,
     OVER = 551,
     OVERLAPS = 552,
     OVERLAY = 553,
     OVERRIDING = 554,
     OWNED = 555,
     OWNER = 556,
     PARALLEL = 557,
     PARSER = 558,
     PARTIAL = 559,
     PARTITION = 560,
     PASSING = 561,
     PASSWORD = 562,
     PERCENT = 563,
     PLACING = 564,
     PLANS = 565,
     POLICY = 566,
     POSITION = 567,
     PRAGMA_P = 568,
     PRECEDING = 569,
     PRECISION = 570,
     PREPARE = 571,
     PREPARED = 572,
     PRESERVE = 573,
     PRIMARY = 574,
     PRIOR = 575,
     PRIVILEGES = 576,
     PROCEDURAL = 577,
     PROCEDURE = 578,
     PROGRAM = 579,
     PUBLICATION = 580,
     QUALIFY = 581,
     QUOTE = 582,
     RANGE = 583,
     READ_P = 584,
     REAL = 585,
     REASSIGN = 586,
     RECHECK = 587,
     RECURSIVE = 588,
     REF = 589,
     REFERENCES = 590,
     REFERENCING = 591,
     REFRESH = 592,
     REINDEX = 593,
     RELATIVE_P = 594,
     RELEASE = 595,
     RENAME = 596,
     REPEATABLE = 597,
     REPLACE = 598,
     REPLICA = 599,
     RESET = 600,
     RESPECT_P = 601,
     RESTART = 602,
     RESTRICT = 603,
     RETURNING = 604,
     RETURNS = 605,
     REVOKE = 606,
     RIGHT = 607,
     ROLE = 608,
     ROLLBACK = 609,
     ROLLUP = 610,
     ROW = 611,
     ROWS = 612,
     RULE = 613,
     SAMPLE = 614,
     SAVEPOINT = 615,
     SCHEMA = 616,
     SCHEMAS = 617,
     SCROLL = 618,
     SEARCH = 619,
     SECOND_P = 620,
     SECONDS_P = 621,
     SECURITY = 622,
     SELECT = 623,
     SEQUENCE = 624,
     SEQUENCES = 625,
     SERIALIZABLE = 626,
     SERVER = 627,
     SESSION = 628,
     SESSION_USER = 629,
     SET = 630,
     SETOF = 631,
     SETS = 632,
     SHARE = 633,
     SHOW = 634,
     SIMILAR = 635,
     SIMPLE = 636,
     SKIP = 637,
     SMALLINT = 638,
     SNAPSHOT = 639,
     SOME = 640,
     SQL_P = 641,
     STABLE = 642,
     STANDALONE_P = 643,
     START = 644,
     STATEMENT = 645,
     STATISTICS = 646,
     STDIN = 647,
     STDOUT = 648,
     STORAGE = 649,
     STORED = 650,
     STRICT_P = 651,
     STRIP_P = 652,
     STRUCT = 653,
     SUBSCRIPTION = 654,
     SUBSTRING = 655,
     SUMMARIZE = 656,
     SYMMETRIC = 657,
     SYSID = 658,
     SYSTEM_P = 659,
     TABLE = 660,
     TABLES = 661,
     TABLESAMPLE = 662,
     TABLESPACE = 663,
     TEMP = 664,
     TEMPLATE = 665,
     TEMPORARY = 666,
     TEXT_P = 667,
     THEN = 668,
     TIME = 669,
     TIMESTAMP = 670,
     TO = 671,
     TRAILING = 672,
     TRANSACTION = 673,
     TRANSFORM = 674,
     TREAT = 675,
     TRIGGER = 676,
     TRIM = 677,
     TRUE_P = 678,
     TRUNCATE = 679,
     TRUSTED = 680,
     TRY_CAST = 681,
     TYPE_P = 682,
     TYPES_P = 683,
     UNBOUNDED = 684,
     UNCOMMITTED = 685,
     UNENCRYPTED = 686,
     UNION = 687,
     UNIQUE = 688,
     UNKNOWN = 689,
     UNLISTEN = 690,
     UNLOGGED = 691,
     UNTIL = 692,
     UPDATE = 693,
     USER = 694,
     USING = 695,
     VACUUM = 696,
     VALID = 697,
     VALIDATE = 698,
     VALIDATOR = 699,
     VALUE_P = 700,
     VALUES = 701,
     VARCHAR = 702,
     VARIADIC = 703,
     VARYING = 704,
     VERBOSE = 705,
     VERSION_P = 706,
     VIEW = 707,
     VIEWS = 708,
     VIRTUAL = 709,
     VOLATILE = 710,
     WHEN = 711,
     WHERE = 712,
     WHITESPACE_P = 713,
     WINDOW = 714,
     WITH = 715,
     WITHIN = 716,
     WITHOUT = 717,
     WORK = 718,
     WRAPPER = 719,
     WRITE_P = 720,
     XML_P = 721,
     XMLATTRIBUTES = 722,
     XMLCONCAT = 723,
     XMLELEMENT = 724,
     XMLEXISTS = 725,
     XMLFOREST = 726,
     XMLNAMESPACES = 727,
     XMLPARSE = 728,
     XMLPI = 729,
     XMLROOT = 730,
     XMLSERIALIZE = 731,
     XMLTABLE = 732,
     YEAR_P = 733,
     YEARS_P = 734,
     YES_P = 735,
     ZONE = 736,
     NOT_LA = 737,
     NULLS_LA = 738,
     WITH_LA = 739,
     POSTFIXOP = 740,
     UMINUS = 741
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
#define POWER_OF 271
#define LESS_EQUALS 272
#define GREATER_EQUALS 273
#define NOT_EQUALS 274
#define ABORT_P 275
#define ABSOLUTE_P 276
#define ACCESS 277
#define ACTION 278
#define ADD_P 279
#define ADMIN 280
#define AFTER 281
#define AGGREGATE 282
#define ALL 283
#define ALSO 284
#define ALTER 285
#define ALWAYS 286
#define ANALYSE 287
#define ANALYZE 288
#define AND 289
#define ANY 290
#define ARRAY 291
#define AS 292
#define ASC_P 293
#define ASSERTION 294
#define ASSIGNMENT 295
#define ASYMMETRIC 296
#define AT 297
#define ATTACH 298
#define ATTRIBUTE 299
#define AUTHORIZATION 300
#define BACKWARD 301
#define BEFORE 302
#define BEGIN_P 303
#define BETWEEN 304
#define BIGINT 305
#define BINARY 306
#define BIT 307
#define BOOLEAN_P 308
#define BOTH 309
#define BY 310
#define CACHE 311
#define CALL_P 312
#define CALLED 313
#define CASCADE 314
#define CASCADED 315
#define CASE 316
#define CAST 317
#define CATALOG_P 318
#define CHAIN 319
#define CHAR_P 320
#define CHARACTER 321
#define CHARACTERISTICS 322
#define CHECK_P 323
#define CHECKPOINT 324
#define CLASS 325
#define CLOSE 326
#define CLUSTER 327
#define COALESCE 328
#define COLLATE 329
#define COLLATION 330
#define COLUMN 331
#define COLUMNS 332
#define COMMENT 333
#define COMMENTS 334
#define COMMIT 335
#define COMMITTED 336
#define COMPRESSION 337
#define CONCURRENTLY 338
#define CONFIGURATION 339
#define CONFLICT 340
#define CONNECTION 341
#define CONSTRAINT 342
#define CONSTRAINTS 343
#define CONTENT_P 344
#define CONTINUE_P 345
#define CONVERSION_P 346
#define COPY 347
#define COST 348
#define CREATE_P 349
#define CROSS 350
#define CSV 351
#define CUBE 352
#define CURRENT_P 353
#define CURRENT_CATALOG 354
#define CURRENT_DATE 355
#define CURRENT_ROLE 356
#define CURRENT_SCHEMA 357
#define CURRENT_TIME 358
#define CURRENT_TIMESTAMP 359
#define CURRENT_USER 360
#define CURSOR 361
#define CYCLE 362
#define DATA_P 363
#define DATABASE 364
#define DAY_P 365
#define DAYS_P 366
#define DEALLOCATE 367
#define DEC 368
#define DECIMAL_P 369
#define DECLARE 370
#define DEFAULT 371
#define DEFAULTS 372
#define DEFERRABLE 373
#define DEFERRED 374
#define DEFINER 375
#define DELETE_P 376
#define DELIMITER 377
#define DELIMITERS 378
#define DEPENDS 379
#define DESC_P 380
#define DESCRIBE 381
#define DETACH 382
#define DICTIONARY 383
#define DISABLE_P 384
#define DISCARD 385
#define DISTINCT 386
#define DO 387
#define DOCUMENT_P 388
#define DOMAIN_P 389
#define DOUBLE_P 390
#define DROP 391
#define EACH 392
#define ELSE 393
#define ENABLE_P 394
#define ENCODING 395
#define ENCRYPTED 396
#define END_P 397
#define ENUM_P 398
#define ESCAPE 399
#define EVENT 400
#define EXCEPT 401
#define EXCLUDE 402
#define EXCLUDING 403
#define EXCLUSIVE 404
#define EXECUTE 405
#define EXISTS 406
#define EXPLAIN 407
#define EXPORT_P 408
#define EXPORT_STATE 409
#define EXTENSION 410
#define EXTERNAL 411
#define EXTRACT 412
#define FALSE_P 413
#define FAMILY 414
#define FETCH 415
#define FILTER 416
#define FIRST_P 417
#define FLOAT_P 418
#define FOLLOWING 419
#define FOR 420
#define FORCE 421
#define FOREIGN 422
#define FORWARD 423
#define FREEZE 424
#define FROM 425
#define FULL 426
#define FUNCTION 427
#define FUNCTIONS 428
#define GENERATED 429
#define GLOB 430
#define GLOBAL 431
#define GRANT 432
#define GRANTED 433
#define GROUP_P 434
#define GROUPING 435
#define GROUPING_ID 436
#define HANDLER 437
#define HAVING 438
#define HEADER_P 439
#define HOLD 440
#define HOUR_P 441
#define HOURS_P 442
#define IDENTITY_P 443
#define IF_P 444
#define IGNORE_P 445
#define ILIKE 446
#define IMMEDIATE 447
#define IMMUTABLE 448
#define IMPLICIT_P 449
#define IMPORT_P 450
#define IN_P 451
#define INCLUDING 452
#define INCREMENT 453
#define INDEX 454
#define INDEXES 455
#define INHERIT 456
#define INHERITS 457
#define INITIALLY 458
#define INLINE_P 459
#define INNER_P 460
#define INOUT 461
#define INPUT_P 462
#define INSENSITIVE 463
#define INSERT 464
#define INSTALL 465
#define INSTEAD 466
#define INT_P 467
#define INTEGER 468
#define INTERSECT 469
#define INTERVAL 470
#define INTO 471
#define INVOKER 472
#define IS 473
#define ISNULL 474
#define ISOLATION 475
#define JOIN 476
#define JSON 477
#define KEY 478
#define LABEL 479
#define LANGUAGE 480
#define LARGE_P 481
#define LAST_P 482
#define LATERAL_P 483
#define LEADING 484
#define LEAKPROOF 485
#define LEFT 486
#define LEVEL 487
#define LIKE 488
#define LIMIT 489
#define LISTEN 490
#define LOAD 491
#define LOCAL 492
#define LOCALTIME 493
#define LOCALTIMESTAMP 494
#define LOCATION 495
#define LOCK_P 496
#define LOCKED 497
#define LOGGED 498
#define MACRO 499
#define MAP 500
#define MAPPING 501
#define MATCH 502
#define MATERIALIZED 503
#define MAXVALUE 504
#define METHOD 505
#define MICROSECOND_P 506
#define MICROSECONDS_P 507
#define MILLISECOND_P 508
#define MILLISECONDS_P 509
#define MINUTE_P 510
#define MINUTES_P 511
#define MINVALUE 512
#define MODE 513
#define MONTH_P 514
#define MONTHS_P 515
#define MOVE 516
#define NAME_P 517
#define NAMES 518
#define NATIONAL 519
#define NATURAL 520
#define NCHAR 521
#define NEW 522
#define NEXT 523
#define NO 524
#define NONE 525
#define NOT 526
#define NOTHING 527
#define NOTIFY 528
#define NOTNULL 529
#define NOWAIT 530
#define NULL_P 531
#define NULLIF 532
#define NULLS_P 533
#define NUMERIC 534
#define OBJECT_P 535
#define OF 536
#define OFF 537
#define OFFSET 538
#define OIDS 539
#define OLD 540
#define ON 541
#define ONLY 542
#define OPERATOR 543
#define OPTION 544
#define OPTIONS 545
#define OR 546
#define ORDER 547
#define ORDINALITY 548
#define OUT_P 549
#define OUTER_P 550
#define OVER 551
#define OVERLAPS 552
#define OVERLAY 553
#define OVERRIDING 554
#define OWNED 555
#define OWNER 556
#define PARALLEL 557
#define PARSER 558
#define PARTIAL 559
#define PARTITION 560
#define PASSING 561
#define PASSWORD 562
#define PERCENT 563
#define PLACING 564
#define PLANS 565
#define POLICY 566
#define POSITION 567
#define PRAGMA_P 568
#define PRECEDING 569
#define PRECISION 570
#define PREPARE 571
#define PREPARED 572
#define PRESERVE 573
#define PRIMARY 574
#define PRIOR 575
#define PRIVILEGES 576
#define PROCEDURAL 577
#define PROCEDURE 578
#define PROGRAM 579
#define PUBLICATION 580
#define QUALIFY 581
#define QUOTE 582
#define RANGE 583
#define READ_P 584
#define REAL 585
#define REASSIGN 586
#define RECHECK 587
#define RECURSIVE 588
#define REF 589
#define REFERENCES 590
#define REFERENCING 591
#define REFRESH 592
#define REINDEX 593
#define RELATIVE_P 594
#define RELEASE 595
#define RENAME 596
#define REPEATABLE 597
#define REPLACE 598
#define REPLICA 599
#define RESET 600
#define RESPECT_P 601
#define RESTART 602
#define RESTRICT 603
#define RETURNING 604
#define RETURNS 605
#define REVOKE 606
#define RIGHT 607
#define ROLE 608
#define ROLLBACK 609
#define ROLLUP 610
#define ROW 611
#define ROWS 612
#define RULE 613
#define SAMPLE 614
#define SAVEPOINT 615
#define SCHEMA 616
#define SCHEMAS 617
#define SCROLL 618
#define SEARCH 619
#define SECOND_P 620
#define SECONDS_P 621
#define SECURITY 622
#define SELECT 623
#define SEQUENCE 624
#define SEQUENCES 625
#define SERIALIZABLE 626
#define SERVER 627
#define SESSION 628
#define SESSION_USER 629
#define SET 630
#define SETOF 631
#define SETS 632
#define SHARE 633
#define SHOW 634
#define SIMILAR 635
#define SIMPLE 636
#define SKIP 637
#define SMALLINT 638
#define SNAPSHOT 639
#define SOME 640
#define SQL_P 641
#define STABLE 642
#define STANDALONE_P 643
#define START 644
#define STATEMENT 645
#define STATISTICS 646
#define STDIN 647
#define STDOUT 648
#define STORAGE 649
#define STORED 650
#define STRICT_P 651
#define STRIP_P 652
#define STRUCT 653
#define SUBSCRIPTION 654
#define SUBSTRING 655
#define SUMMARIZE 656
#define SYMMETRIC 657
#define SYSID 658
#define SYSTEM_P 659
#define TABLE 660
#define TABLES 661
#define TABLESAMPLE 662
#define TABLESPACE 663
#define TEMP 664
#define TEMPLATE 665
#define TEMPORARY 666
#define TEXT_P 667
#define THEN 668
#define TIME 669
#define TIMESTAMP 670
#define TO 671
#define TRAILING 672
#define TRANSACTION 673
#define TRANSFORM 674
#define TREAT 675
#define TRIGGER 676
#define TRIM 677
#define TRUE_P 678
#define TRUNCATE 679
#define TRUSTED 680
#define TRY_CAST 681
#define TYPE_P 682
#define TYPES_P 683
#define UNBOUNDED 684
#define UNCOMMITTED 685
#define UNENCRYPTED 686
#define UNION 687
#define UNIQUE 688
#define UNKNOWN 689
#define UNLISTEN 690
#define UNLOGGED 691
#define UNTIL 692
#define UPDATE 693
#define USER 694
#define USING 695
#define VACUUM 696
#define VALID 697
#define VALIDATE 698
#define VALIDATOR 699
#define VALUE_P 700
#define VALUES 701
#define VARCHAR 702
#define VARIADIC 703
#define VARYING 704
#define VERBOSE 705
#define VERSION_P 706
#define VIEW 707
#define VIEWS 708
#define VIRTUAL 709
#define VOLATILE 710
#define WHEN 711
#define WHERE 712
#define WHITESPACE_P 713
#define WINDOW 714
#define WITH 715
#define WITHIN 716
#define WITHOUT 717
#define WORK 718
#define WRAPPER 719
#define WRITE_P 720
#define XML_P 721
#define XMLATTRIBUTES 722
#define XMLCONCAT 723
#define XMLELEMENT 724
#define XMLEXISTS 725
#define XMLFOREST 726
#define XMLNAMESPACES 727
#define XMLPARSE 728
#define XMLPI 729
#define XMLROOT 730
#define XMLSERIALIZE 731
#define XMLTABLE 732
#define YEAR_P 733
#define YEARS_P 734
#define YES_P 735
#define ZONE 736
#define NOT_LA 737
#define NULLS_LA 738
#define WITH_LA 739
#define POSTFIXOP 740
#define UMINUS 741




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
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
}
/* Line 1529 of yacc.c.  */
#line 1066 "third_party/libpg_query/grammar/grammar_out.hpp"
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


