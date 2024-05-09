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
     INTEGER_DIVISION = 270,
     POWER_OF = 271,
     LAMBDA_ARROW = 272,
     DOUBLE_ARROW = 273,
     LESS_EQUALS = 274,
     GREATER_EQUALS = 275,
     NOT_EQUALS = 276,
     ABORT_P = 277,
     ABSOLUTE_P = 278,
     ACCESS = 279,
     ACTION = 280,
     ADD_P = 281,
     ADMIN = 282,
     AFTER = 283,
     AGGREGATE = 284,
     ALL = 285,
     ALSO = 286,
     ALTER = 287,
     ALWAYS = 288,
     ANALYSE = 289,
     ANALYZE = 290,
     AND = 291,
     ANTI = 292,
     ANY = 293,
     ARRAY = 294,
     AS = 295,
     ASC_P = 296,
     ASOF = 297,
     ASSERTION = 298,
     ASSIGNMENT = 299,
     ASYMMETRIC = 300,
     AT = 301,
     ATTACH = 302,
     ATTRIBUTE = 303,
     AUTHORIZATION = 304,
     BACKWARD = 305,
     BEFORE = 306,
     BEGIN_P = 307,
     BETWEEN = 308,
     BIGINT = 309,
     BINARY = 310,
     BIT = 311,
     BOOLEAN_P = 312,
     BOTH = 313,
     BY = 314,
     CACHE = 315,
     CALL_P = 316,
     CALLED = 317,
     CASCADE = 318,
     CASCADED = 319,
     CASE = 320,
     CAST = 321,
     CATALOG_P = 322,
     CENTURIES_P = 323,
     CENTURY_P = 324,
     CHAIN = 325,
     CHAR_P = 326,
     CHARACTER = 327,
     CHARACTERISTICS = 328,
     CHECK_P = 329,
     CHECKPOINT = 330,
     CLASS = 331,
     CLOSE = 332,
     CLUSTER = 333,
     COALESCE = 334,
     COLLATE = 335,
     COLLATION = 336,
     COLUMN = 337,
     COLUMNS = 338,
     COMMENT = 339,
     COMMENTS = 340,
     COMMIT = 341,
     COMMITTED = 342,
     COMPRESSION = 343,
     CONCURRENTLY = 344,
     CONFIGURATION = 345,
     CONFLICT = 346,
     CONNECTION = 347,
     CONSTRAINT = 348,
     CONSTRAINTS = 349,
     CONTENT_P = 350,
     CONTINUE_P = 351,
     CONVERSION_P = 352,
     COPY = 353,
     COST = 354,
     CREATE_P = 355,
     CROSS = 356,
     CSV = 357,
     CUBE = 358,
     CURRENT_P = 359,
     CURSOR = 360,
     CYCLE = 361,
     DATA_P = 362,
     DATABASE = 363,
     DAY_P = 364,
     DAYS_P = 365,
     DEALLOCATE = 366,
     DEC = 367,
     DECADE_P = 368,
     DECADES_P = 369,
     DECIMAL_P = 370,
     DECLARE = 371,
     DEFAULT = 372,
     DEFAULTS = 373,
     DEFERRABLE = 374,
     DEFERRED = 375,
     DEFINER = 376,
     DELETE_P = 377,
     DELIMITER = 378,
     DELIMITERS = 379,
     DEPENDS = 380,
     DESC_P = 381,
     DESCRIBE = 382,
     DETACH = 383,
     DICTIONARY = 384,
     DISABLE_P = 385,
     DISCARD = 386,
     DISTINCT = 387,
     DO = 388,
     DOCUMENT_P = 389,
     DOMAIN_P = 390,
     DOUBLE_P = 391,
     DROP = 392,
     EACH = 393,
     ELSE = 394,
     ENABLE_P = 395,
     ENCODING = 396,
     ENCRYPTED = 397,
     END_P = 398,
     ENUM_P = 399,
     ESCAPE = 400,
     EVENT = 401,
     EXCEPT = 402,
     EXCLUDE = 403,
     EXCLUDING = 404,
     EXCLUSIVE = 405,
     EXECUTE = 406,
     EXISTS = 407,
     EXPLAIN = 408,
     EXPORT_P = 409,
     EXPORT_STATE = 410,
     EXTENSION = 411,
     EXTERNAL = 412,
     EXTRACT = 413,
     FALSE_P = 414,
     FAMILY = 415,
     FETCH = 416,
     FILTER = 417,
     FIRST_P = 418,
     FLOAT_P = 419,
     FOLLOWING = 420,
     FOR = 421,
     FORCE = 422,
     FOREIGN = 423,
     FORWARD = 424,
     FREEZE = 425,
     FROM = 426,
     FULL = 427,
     FUNCTION = 428,
     FUNCTIONS = 429,
     GENERATED = 430,
     GLOB = 431,
     GLOBAL = 432,
     GRANT = 433,
     GRANTED = 434,
     GROUP_P = 435,
     GROUPING = 436,
     GROUPING_ID = 437,
     GROUPS = 438,
     HANDLER = 439,
     HAVING = 440,
     HEADER_P = 441,
     HOLD = 442,
     HOUR_P = 443,
     HOURS_P = 444,
     IDENTITY_P = 445,
     IF_P = 446,
     IGNORE_P = 447,
     ILIKE = 448,
     IMMEDIATE = 449,
     IMMUTABLE = 450,
     IMPLICIT_P = 451,
     IMPORT_P = 452,
     IN_P = 453,
     INCLUDE_P = 454,
     INCLUDING = 455,
     INCREMENT = 456,
     INDEX = 457,
     INDEXES = 458,
     INHERIT = 459,
     INHERITS = 460,
     INITIALLY = 461,
     INLINE_P = 462,
     INNER_P = 463,
     INOUT = 464,
     INPUT_P = 465,
     INSENSITIVE = 466,
     INSERT = 467,
     INSTALL = 468,
     INSTEAD = 469,
     INT_P = 470,
     INTEGER = 471,
     INTERSECT = 472,
     INTERVAL = 473,
     INTO = 474,
     INVOKER = 475,
     IS = 476,
     ISNULL = 477,
     ISOLATION = 478,
     JOIN = 479,
     JSON = 480,
     KEY = 481,
     LABEL = 482,
     LANGUAGE = 483,
     LARGE_P = 484,
     LAST_P = 485,
     LATERAL_P = 486,
     LEADING = 487,
     LEAKPROOF = 488,
     LEFT = 489,
     LEVEL = 490,
     LIKE = 491,
     LIMIT = 492,
     LISTEN = 493,
     LOAD = 494,
     LOCAL = 495,
     LOCATION = 496,
     LOCK_P = 497,
     LOCKED = 498,
     LOGGED = 499,
     MACRO = 500,
     MAP = 501,
     MAPPING = 502,
     MATCH = 503,
     MATERIALIZED = 504,
     MAXVALUE = 505,
     METHOD = 506,
     MICROSECOND_P = 507,
     MICROSECONDS_P = 508,
     MILLENNIA_P = 509,
     MILLENNIUM_P = 510,
     MILLISECOND_P = 511,
     MILLISECONDS_P = 512,
     MINUTE_P = 513,
     MINUTES_P = 514,
     MINVALUE = 515,
     MODE = 516,
     MONTH_P = 517,
     MONTHS_P = 518,
     MOVE = 519,
     NAME_P = 520,
     NAMES = 521,
     NATIONAL = 522,
     NATURAL = 523,
     NCHAR = 524,
     NEW = 525,
     NEXT = 526,
     NO = 527,
     NONE = 528,
     NOT = 529,
     NOTHING = 530,
     NOTIFY = 531,
     NOTNULL = 532,
     NOWAIT = 533,
     NULL_P = 534,
     NULLIF = 535,
     NULLS_P = 536,
     NUMERIC = 537,
     OBJECT_P = 538,
     OF = 539,
     OFF = 540,
     OFFSET = 541,
     OIDS = 542,
     OLD = 543,
     ON = 544,
     ONLY = 545,
     OPERATOR = 546,
     OPTION = 547,
     OPTIONS = 548,
     OR = 549,
     ORDER = 550,
     ORDINALITY = 551,
     OTHERS = 552,
     OUT_P = 553,
     OUTER_P = 554,
     OVER = 555,
     OVERLAPS = 556,
     OVERLAY = 557,
     OVERRIDING = 558,
     OWNED = 559,
     OWNER = 560,
     PARALLEL = 561,
     PARSER = 562,
     PARTIAL = 563,
     PARTITION = 564,
     PASSING = 565,
     PASSWORD = 566,
     PERCENT = 567,
     PERSISTENT = 568,
     PIVOT = 569,
     PIVOT_LONGER = 570,
     PIVOT_WIDER = 571,
     PLACING = 572,
     PLANS = 573,
     POLICY = 574,
     POSITION = 575,
     POSITIONAL = 576,
     PRAGMA_P = 577,
     PRECEDING = 578,
     PRECISION = 579,
     PREPARE = 580,
     PREPARED = 581,
     PRESERVE = 582,
     PRIMARY = 583,
     PRIOR = 584,
     PRIVILEGES = 585,
     PROCEDURAL = 586,
     PROCEDURE = 587,
     PROGRAM = 588,
     PUBLICATION = 589,
     QUALIFY = 590,
     QUARTER_P = 591,
     QUARTERS_P = 592,
     QUOTE = 593,
     RANGE = 594,
     READ_P = 595,
     REAL = 596,
     REASSIGN = 597,
     RECHECK = 598,
     RECURSIVE = 599,
     REF = 600,
     REFERENCES = 601,
     REFERENCING = 602,
     REFRESH = 603,
     REINDEX = 604,
     RELATIVE_P = 605,
     RELEASE = 606,
     RENAME = 607,
     REPEATABLE = 608,
     REPLACE = 609,
     REPLICA = 610,
     RESET = 611,
     RESPECT_P = 612,
     RESTART = 613,
     RESTRICT = 614,
     RETURNING = 615,
     RETURNS = 616,
     REVOKE = 617,
     RIGHT = 618,
     ROLE = 619,
     ROLLBACK = 620,
     ROLLUP = 621,
     ROW = 622,
     ROWS = 623,
     RULE = 624,
     SAMPLE = 625,
     SAVEPOINT = 626,
     SCHEMA = 627,
     SCHEMAS = 628,
     SCOPE = 629,
     SCROLL = 630,
     SEARCH = 631,
     SECOND_P = 632,
     SECONDS_P = 633,
     SECRET = 634,
     SECURITY = 635,
     SELECT = 636,
     SEMI = 637,
     SEQUENCE = 638,
     SEQUENCES = 639,
     SERIALIZABLE = 640,
     SERVER = 641,
     SESSION = 642,
     SET = 643,
     SETOF = 644,
     SETS = 645,
     SHARE = 646,
     SHOW = 647,
     SIMILAR = 648,
     SIMPLE = 649,
     SKIP = 650,
     SMALLINT = 651,
     SNAPSHOT = 652,
     SOME = 653,
     SQL_P = 654,
     STABLE = 655,
     STANDALONE_P = 656,
     START = 657,
     STATEMENT = 658,
     STATISTICS = 659,
     STDIN = 660,
     STDOUT = 661,
     STORAGE = 662,
     STORED = 663,
     STRICT_P = 664,
     STRIP_P = 665,
     STRUCT = 666,
     SUBSCRIPTION = 667,
     SUBSTRING = 668,
     SUMMARIZE = 669,
     SYMMETRIC = 670,
     SYSID = 671,
     SYSTEM_P = 672,
     TABLE = 673,
     TABLES = 674,
     TABLESAMPLE = 675,
     TABLESPACE = 676,
     TEMP = 677,
     TEMPLATE = 678,
     TEMPORARY = 679,
     TEXT_P = 680,
     THEN = 681,
     TIES = 682,
     TIME = 683,
     TIMESTAMP = 684,
     TO = 685,
     TRAILING = 686,
     TRANSACTION = 687,
     TRANSFORM = 688,
     TREAT = 689,
     TRIGGER = 690,
     TRIM = 691,
     TRUE_P = 692,
     TRUNCATE = 693,
     TRUSTED = 694,
     TRY_CAST = 695,
     TYPE_P = 696,
     TYPES_P = 697,
     UNBOUNDED = 698,
     UNCOMMITTED = 699,
     UNENCRYPTED = 700,
     UNION = 701,
     UNIQUE = 702,
     UNKNOWN = 703,
     UNLISTEN = 704,
     UNLOGGED = 705,
     UNPIVOT = 706,
     UNTIL = 707,
     UPDATE = 708,
     USE_P = 709,
     USER = 710,
     USING = 711,
     VACUUM = 712,
     VALID = 713,
     VALIDATE = 714,
     VALIDATOR = 715,
     VALUE_P = 716,
     VALUES = 717,
     VARCHAR = 718,
     VARIADIC = 719,
     VARYING = 720,
     VERBOSE = 721,
     VERSION_P = 722,
     VIEW = 723,
     VIEWS = 724,
     VIRTUAL = 725,
     VOLATILE = 726,
     WEEK_P = 727,
     WEEKS_P = 728,
     WHEN = 729,
     WHERE = 730,
     WHITESPACE_P = 731,
     WINDOW = 732,
     WITH = 733,
     WITHIN = 734,
     WITHOUT = 735,
     WORK = 736,
     WRAPPER = 737,
     WRITE_P = 738,
     XML_P = 739,
     XMLATTRIBUTES = 740,
     XMLCONCAT = 741,
     XMLELEMENT = 742,
     XMLEXISTS = 743,
     XMLFOREST = 744,
     XMLNAMESPACES = 745,
     XMLPARSE = 746,
     XMLPI = 747,
     XMLROOT = 748,
     XMLSERIALIZE = 749,
     XMLTABLE = 750,
     YEAR_P = 751,
     YEARS_P = 752,
     YES_P = 753,
     ZONE = 754,
     NOT_LA = 755,
     NULLS_LA = 756,
     WITH_LA = 757,
     POSTFIXOP = 758,
     UMINUS = 759
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
#define INTEGER_DIVISION 270
#define POWER_OF 271
#define LAMBDA_ARROW 272
#define DOUBLE_ARROW 273
#define LESS_EQUALS 274
#define GREATER_EQUALS 275
#define NOT_EQUALS 276
#define ABORT_P 277
#define ABSOLUTE_P 278
#define ACCESS 279
#define ACTION 280
#define ADD_P 281
#define ADMIN 282
#define AFTER 283
#define AGGREGATE 284
#define ALL 285
#define ALSO 286
#define ALTER 287
#define ALWAYS 288
#define ANALYSE 289
#define ANALYZE 290
#define AND 291
#define ANTI 292
#define ANY 293
#define ARRAY 294
#define AS 295
#define ASC_P 296
#define ASOF 297
#define ASSERTION 298
#define ASSIGNMENT 299
#define ASYMMETRIC 300
#define AT 301
#define ATTACH 302
#define ATTRIBUTE 303
#define AUTHORIZATION 304
#define BACKWARD 305
#define BEFORE 306
#define BEGIN_P 307
#define BETWEEN 308
#define BIGINT 309
#define BINARY 310
#define BIT 311
#define BOOLEAN_P 312
#define BOTH 313
#define BY 314
#define CACHE 315
#define CALL_P 316
#define CALLED 317
#define CASCADE 318
#define CASCADED 319
#define CASE 320
#define CAST 321
#define CATALOG_P 322
#define CENTURIES_P 323
#define CENTURY_P 324
#define CHAIN 325
#define CHAR_P 326
#define CHARACTER 327
#define CHARACTERISTICS 328
#define CHECK_P 329
#define CHECKPOINT 330
#define CLASS 331
#define CLOSE 332
#define CLUSTER 333
#define COALESCE 334
#define COLLATE 335
#define COLLATION 336
#define COLUMN 337
#define COLUMNS 338
#define COMMENT 339
#define COMMENTS 340
#define COMMIT 341
#define COMMITTED 342
#define COMPRESSION 343
#define CONCURRENTLY 344
#define CONFIGURATION 345
#define CONFLICT 346
#define CONNECTION 347
#define CONSTRAINT 348
#define CONSTRAINTS 349
#define CONTENT_P 350
#define CONTINUE_P 351
#define CONVERSION_P 352
#define COPY 353
#define COST 354
#define CREATE_P 355
#define CROSS 356
#define CSV 357
#define CUBE 358
#define CURRENT_P 359
#define CURSOR 360
#define CYCLE 361
#define DATA_P 362
#define DATABASE 363
#define DAY_P 364
#define DAYS_P 365
#define DEALLOCATE 366
#define DEC 367
#define DECADE_P 368
#define DECADES_P 369
#define DECIMAL_P 370
#define DECLARE 371
#define DEFAULT 372
#define DEFAULTS 373
#define DEFERRABLE 374
#define DEFERRED 375
#define DEFINER 376
#define DELETE_P 377
#define DELIMITER 378
#define DELIMITERS 379
#define DEPENDS 380
#define DESC_P 381
#define DESCRIBE 382
#define DETACH 383
#define DICTIONARY 384
#define DISABLE_P 385
#define DISCARD 386
#define DISTINCT 387
#define DO 388
#define DOCUMENT_P 389
#define DOMAIN_P 390
#define DOUBLE_P 391
#define DROP 392
#define EACH 393
#define ELSE 394
#define ENABLE_P 395
#define ENCODING 396
#define ENCRYPTED 397
#define END_P 398
#define ENUM_P 399
#define ESCAPE 400
#define EVENT 401
#define EXCEPT 402
#define EXCLUDE 403
#define EXCLUDING 404
#define EXCLUSIVE 405
#define EXECUTE 406
#define EXISTS 407
#define EXPLAIN 408
#define EXPORT_P 409
#define EXPORT_STATE 410
#define EXTENSION 411
#define EXTERNAL 412
#define EXTRACT 413
#define FALSE_P 414
#define FAMILY 415
#define FETCH 416
#define FILTER 417
#define FIRST_P 418
#define FLOAT_P 419
#define FOLLOWING 420
#define FOR 421
#define FORCE 422
#define FOREIGN 423
#define FORWARD 424
#define FREEZE 425
#define FROM 426
#define FULL 427
#define FUNCTION 428
#define FUNCTIONS 429
#define GENERATED 430
#define GLOB 431
#define GLOBAL 432
#define GRANT 433
#define GRANTED 434
#define GROUP_P 435
#define GROUPING 436
#define GROUPING_ID 437
#define GROUPS 438
#define HANDLER 439
#define HAVING 440
#define HEADER_P 441
#define HOLD 442
#define HOUR_P 443
#define HOURS_P 444
#define IDENTITY_P 445
#define IF_P 446
#define IGNORE_P 447
#define ILIKE 448
#define IMMEDIATE 449
#define IMMUTABLE 450
#define IMPLICIT_P 451
#define IMPORT_P 452
#define IN_P 453
#define INCLUDE_P 454
#define INCLUDING 455
#define INCREMENT 456
#define INDEX 457
#define INDEXES 458
#define INHERIT 459
#define INHERITS 460
#define INITIALLY 461
#define INLINE_P 462
#define INNER_P 463
#define INOUT 464
#define INPUT_P 465
#define INSENSITIVE 466
#define INSERT 467
#define INSTALL 468
#define INSTEAD 469
#define INT_P 470
#define INTEGER 471
#define INTERSECT 472
#define INTERVAL 473
#define INTO 474
#define INVOKER 475
#define IS 476
#define ISNULL 477
#define ISOLATION 478
#define JOIN 479
#define JSON 480
#define KEY 481
#define LABEL 482
#define LANGUAGE 483
#define LARGE_P 484
#define LAST_P 485
#define LATERAL_P 486
#define LEADING 487
#define LEAKPROOF 488
#define LEFT 489
#define LEVEL 490
#define LIKE 491
#define LIMIT 492
#define LISTEN 493
#define LOAD 494
#define LOCAL 495
#define LOCATION 496
#define LOCK_P 497
#define LOCKED 498
#define LOGGED 499
#define MACRO 500
#define MAP 501
#define MAPPING 502
#define MATCH 503
#define MATERIALIZED 504
#define MAXVALUE 505
#define METHOD 506
#define MICROSECOND_P 507
#define MICROSECONDS_P 508
#define MILLENNIA_P 509
#define MILLENNIUM_P 510
#define MILLISECOND_P 511
#define MILLISECONDS_P 512
#define MINUTE_P 513
#define MINUTES_P 514
#define MINVALUE 515
#define MODE 516
#define MONTH_P 517
#define MONTHS_P 518
#define MOVE 519
#define NAME_P 520
#define NAMES 521
#define NATIONAL 522
#define NATURAL 523
#define NCHAR 524
#define NEW 525
#define NEXT 526
#define NO 527
#define NONE 528
#define NOT 529
#define NOTHING 530
#define NOTIFY 531
#define NOTNULL 532
#define NOWAIT 533
#define NULL_P 534
#define NULLIF 535
#define NULLS_P 536
#define NUMERIC 537
#define OBJECT_P 538
#define OF 539
#define OFF 540
#define OFFSET 541
#define OIDS 542
#define OLD 543
#define ON 544
#define ONLY 545
#define OPERATOR 546
#define OPTION 547
#define OPTIONS 548
#define OR 549
#define ORDER 550
#define ORDINALITY 551
#define OTHERS 552
#define OUT_P 553
#define OUTER_P 554
#define OVER 555
#define OVERLAPS 556
#define OVERLAY 557
#define OVERRIDING 558
#define OWNED 559
#define OWNER 560
#define PARALLEL 561
#define PARSER 562
#define PARTIAL 563
#define PARTITION 564
#define PASSING 565
#define PASSWORD 566
#define PERCENT 567
#define PERSISTENT 568
#define PIVOT 569
#define PIVOT_LONGER 570
#define PIVOT_WIDER 571
#define PLACING 572
#define PLANS 573
#define POLICY 574
#define POSITION 575
#define POSITIONAL 576
#define PRAGMA_P 577
#define PRECEDING 578
#define PRECISION 579
#define PREPARE 580
#define PREPARED 581
#define PRESERVE 582
#define PRIMARY 583
#define PRIOR 584
#define PRIVILEGES 585
#define PROCEDURAL 586
#define PROCEDURE 587
#define PROGRAM 588
#define PUBLICATION 589
#define QUALIFY 590
#define QUARTER_P 591
#define QUARTERS_P 592
#define QUOTE 593
#define RANGE 594
#define READ_P 595
#define REAL 596
#define REASSIGN 597
#define RECHECK 598
#define RECURSIVE 599
#define REF 600
#define REFERENCES 601
#define REFERENCING 602
#define REFRESH 603
#define REINDEX 604
#define RELATIVE_P 605
#define RELEASE 606
#define RENAME 607
#define REPEATABLE 608
#define REPLACE 609
#define REPLICA 610
#define RESET 611
#define RESPECT_P 612
#define RESTART 613
#define RESTRICT 614
#define RETURNING 615
#define RETURNS 616
#define REVOKE 617
#define RIGHT 618
#define ROLE 619
#define ROLLBACK 620
#define ROLLUP 621
#define ROW 622
#define ROWS 623
#define RULE 624
#define SAMPLE 625
#define SAVEPOINT 626
#define SCHEMA 627
#define SCHEMAS 628
#define SCOPE 629
#define SCROLL 630
#define SEARCH 631
#define SECOND_P 632
#define SECONDS_P 633
#define SECRET 634
#define SECURITY 635
#define SELECT 636
#define SEMI 637
#define SEQUENCE 638
#define SEQUENCES 639
#define SERIALIZABLE 640
#define SERVER 641
#define SESSION 642
#define SET 643
#define SETOF 644
#define SETS 645
#define SHARE 646
#define SHOW 647
#define SIMILAR 648
#define SIMPLE 649
#define SKIP 650
#define SMALLINT 651
#define SNAPSHOT 652
#define SOME 653
#define SQL_P 654
#define STABLE 655
#define STANDALONE_P 656
#define START 657
#define STATEMENT 658
#define STATISTICS 659
#define STDIN 660
#define STDOUT 661
#define STORAGE 662
#define STORED 663
#define STRICT_P 664
#define STRIP_P 665
#define STRUCT 666
#define SUBSCRIPTION 667
#define SUBSTRING 668
#define SUMMARIZE 669
#define SYMMETRIC 670
#define SYSID 671
#define SYSTEM_P 672
#define TABLE 673
#define TABLES 674
#define TABLESAMPLE 675
#define TABLESPACE 676
#define TEMP 677
#define TEMPLATE 678
#define TEMPORARY 679
#define TEXT_P 680
#define THEN 681
#define TIES 682
#define TIME 683
#define TIMESTAMP 684
#define TO 685
#define TRAILING 686
#define TRANSACTION 687
#define TRANSFORM 688
#define TREAT 689
#define TRIGGER 690
#define TRIM 691
#define TRUE_P 692
#define TRUNCATE 693
#define TRUSTED 694
#define TRY_CAST 695
#define TYPE_P 696
#define TYPES_P 697
#define UNBOUNDED 698
#define UNCOMMITTED 699
#define UNENCRYPTED 700
#define UNION 701
#define UNIQUE 702
#define UNKNOWN 703
#define UNLISTEN 704
#define UNLOGGED 705
#define UNPIVOT 706
#define UNTIL 707
#define UPDATE 708
#define USE_P 709
#define USER 710
#define USING 711
#define VACUUM 712
#define VALID 713
#define VALIDATE 714
#define VALIDATOR 715
#define VALUE_P 716
#define VALUES 717
#define VARCHAR 718
#define VARIADIC 719
#define VARYING 720
#define VERBOSE 721
#define VERSION_P 722
#define VIEW 723
#define VIEWS 724
#define VIRTUAL 725
#define VOLATILE 726
#define WEEK_P 727
#define WEEKS_P 728
#define WHEN 729
#define WHERE 730
#define WHITESPACE_P 731
#define WINDOW 732
#define WITH 733
#define WITHIN 734
#define WITHOUT 735
#define WORK 736
#define WRAPPER 737
#define WRITE_P 738
#define XML_P 739
#define XMLATTRIBUTES 740
#define XMLCONCAT 741
#define XMLELEMENT 742
#define XMLEXISTS 743
#define XMLFOREST 744
#define XMLNAMESPACES 745
#define XMLPARSE 746
#define XMLPI 747
#define XMLROOT 748
#define XMLSERIALIZE 749
#define XMLTABLE 750
#define YEAR_P 751
#define YEARS_P 752
#define YES_P 753
#define ZONE 754
#define NOT_LA 755
#define NULLS_LA 756
#define WITH_LA 757
#define POSTFIXOP 758
#define UMINUS 759




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
	PGCTEMaterialize			ctematerialize;
	PGWithClause			*with;
	PGInferClause			*infer;
	PGOnConflictClause	*onconflict;
	PGOnConflictActionAlias onconflictshorthand;
	PGAIndices			*aind;
	PGResTarget			*target;
	PGInsertStmt			*istmt;
	PGVariableSetStmt		*vsetstmt;
	PGOverridingKind       override;
	PGSortByDir            sortorder;
	PGSortByNulls          nullorder;
	PGIgnoreNulls          ignorenulls;
	PGConstrType           constr;
	PGLockClauseStrength lockstrength;
	PGLockWaitPolicy lockwaitpolicy;
	PGSubLinkType subquerytype;
	PGViewCheckOption viewcheckoption;
	PGInsertColumnOrder bynameorposition;
}
/* Line 1529 of yacc.c.  */
#line 1106 "third_party/libpg_query/grammar/grammar_out.hpp"
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


