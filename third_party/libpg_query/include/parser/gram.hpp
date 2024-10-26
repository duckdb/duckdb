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
     EXTENSIONS = 412,
     EXTERNAL = 413,
     EXTRACT = 414,
     FALSE_P = 415,
     FAMILY = 416,
     FETCH = 417,
     FILTER = 418,
     FIRST_P = 419,
     FLOAT_P = 420,
     FOLLOWING = 421,
     FOR = 422,
     FORCE = 423,
     FOREIGN = 424,
     FORWARD = 425,
     FREEZE = 426,
     FROM = 427,
     FULL = 428,
     FUNCTION = 429,
     FUNCTIONS = 430,
     GENERATED = 431,
     GLOB = 432,
     GLOBAL = 433,
     GRANT = 434,
     GRANTED = 435,
     GROUP_P = 436,
     GROUPING = 437,
     GROUPING_ID = 438,
     GROUPS = 439,
     HANDLER = 440,
     HAVING = 441,
     HEADER_P = 442,
     HOLD = 443,
     HOUR_P = 444,
     HOURS_P = 445,
     IDENTITY_P = 446,
     IF_P = 447,
     IGNORE_P = 448,
     ILIKE = 449,
     IMMEDIATE = 450,
     IMMUTABLE = 451,
     IMPLICIT_P = 452,
     IMPORT_P = 453,
     IN_P = 454,
     INCLUDE_P = 455,
     INCLUDING = 456,
     INCREMENT = 457,
     INDEX = 458,
     INDEXES = 459,
     INHERIT = 460,
     INHERITS = 461,
     INITIALLY = 462,
     INLINE_P = 463,
     INNER_P = 464,
     INOUT = 465,
     INPUT_P = 466,
     INSENSITIVE = 467,
     INSERT = 468,
     INSTALL = 469,
     INSTEAD = 470,
     INT_P = 471,
     INTEGER = 472,
     INTERSECT = 473,
     INTERVAL = 474,
     INTO = 475,
     INVOKER = 476,
     IS = 477,
     ISNULL = 478,
     ISOLATION = 479,
     JOIN = 480,
     JSON = 481,
     KEY = 482,
     LABEL = 483,
     LANGUAGE = 484,
     LARGE_P = 485,
     LAST_P = 486,
     LATERAL_P = 487,
     LEADING = 488,
     LEAKPROOF = 489,
     LEFT = 490,
     LEVEL = 491,
     LIKE = 492,
     LIMIT = 493,
     LISTEN = 494,
     LOAD = 495,
     LOCAL = 496,
     LOCATION = 497,
     LOCK_P = 498,
     LOCKED = 499,
     LOGGED = 500,
     MACRO = 501,
     MAP = 502,
     MAPPING = 503,
     MATCH = 504,
     MATERIALIZED = 505,
     MAXVALUE = 506,
     METHOD = 507,
     MICROSECOND_P = 508,
     MICROSECONDS_P = 509,
     MILLENNIA_P = 510,
     MILLENNIUM_P = 511,
     MILLISECOND_P = 512,
     MILLISECONDS_P = 513,
     MINUTE_P = 514,
     MINUTES_P = 515,
     MINVALUE = 516,
     MODE = 517,
     MONTH_P = 518,
     MONTHS_P = 519,
     MOVE = 520,
     NAME_P = 521,
     NAMES = 522,
     NATIONAL = 523,
     NATURAL = 524,
     NCHAR = 525,
     NEW = 526,
     NEXT = 527,
     NO = 528,
     NONE = 529,
     NOT = 530,
     NOTHING = 531,
     NOTIFY = 532,
     NOTNULL = 533,
     NOWAIT = 534,
     NULL_P = 535,
     NULLIF = 536,
     NULLS_P = 537,
     NUMERIC = 538,
     OBJECT_P = 539,
     OF = 540,
     OFF = 541,
     OFFSET = 542,
     OIDS = 543,
     OLD = 544,
     ON = 545,
     ONLY = 546,
     OPERATOR = 547,
     OPTION = 548,
     OPTIONS = 549,
     OR = 550,
     ORDER = 551,
     ORDINALITY = 552,
     OTHERS = 553,
     OUT_P = 554,
     OUTER_P = 555,
     OVER = 556,
     OVERLAPS = 557,
     OVERLAY = 558,
     OVERRIDING = 559,
     OWNED = 560,
     OWNER = 561,
     PARALLEL = 562,
     PARSER = 563,
     PARTIAL = 564,
     PARTITION = 565,
     PASSING = 566,
     PASSWORD = 567,
     PERCENT = 568,
     PERSISTENT = 569,
     PIVOT = 570,
     PIVOT_LONGER = 571,
     PIVOT_WIDER = 572,
     PLACING = 573,
     PLANS = 574,
     POLICY = 575,
     POSITION = 576,
     POSITIONAL = 577,
     PRAGMA_P = 578,
     PRECEDING = 579,
     PRECISION = 580,
     PREPARE = 581,
     PREPARED = 582,
     PRESERVE = 583,
     PRIMARY = 584,
     PRIOR = 585,
     PRIVILEGES = 586,
     PROCEDURAL = 587,
     PROCEDURE = 588,
     PROGRAM = 589,
     PUBLICATION = 590,
     QUALIFY = 591,
     QUARTER_P = 592,
     QUARTERS_P = 593,
     QUOTE = 594,
     RANGE = 595,
     READ_P = 596,
     REAL = 597,
     REASSIGN = 598,
     RECHECK = 599,
     RECURSIVE = 600,
     REF = 601,
     REFERENCES = 602,
     REFERENCING = 603,
     REFRESH = 604,
     REINDEX = 605,
     RELATIVE_P = 606,
     RELEASE = 607,
     RENAME = 608,
     REPEATABLE = 609,
     REPLACE = 610,
     REPLICA = 611,
     RESET = 612,
     RESPECT_P = 613,
     RESTART = 614,
     RESTRICT = 615,
     RETURNING = 616,
     RETURNS = 617,
     REVOKE = 618,
     RIGHT = 619,
     ROLE = 620,
     ROLLBACK = 621,
     ROLLUP = 622,
     ROW = 623,
     ROWS = 624,
     RULE = 625,
     SAMPLE = 626,
     SAVEPOINT = 627,
     SCHEMA = 628,
     SCHEMAS = 629,
     SCOPE = 630,
     SCROLL = 631,
     SEARCH = 632,
     SECOND_P = 633,
     SECONDS_P = 634,
     SECRET = 635,
     SECURITY = 636,
     SELECT = 637,
     SEMI = 638,
     SEQUENCE = 639,
     SEQUENCES = 640,
     SERIALIZABLE = 641,
     SERVER = 642,
     SESSION = 643,
     SET = 644,
     SETOF = 645,
     SETS = 646,
     SHARE = 647,
     SHOW = 648,
     SIMILAR = 649,
     SIMPLE = 650,
     SKIP = 651,
     SMALLINT = 652,
     SNAPSHOT = 653,
     SOME = 654,
     SQL_P = 655,
     STABLE = 656,
     STANDALONE_P = 657,
     START = 658,
     STATEMENT = 659,
     STATISTICS = 660,
     STDIN = 661,
     STDOUT = 662,
     STORAGE = 663,
     STORED = 664,
     STRICT_P = 665,
     STRIP_P = 666,
     STRUCT = 667,
     SUBSCRIPTION = 668,
     SUBSTRING = 669,
     SUMMARIZE = 670,
     SYMMETRIC = 671,
     SYSID = 672,
     SYSTEM_P = 673,
     TABLE = 674,
     TABLES = 675,
     TABLESAMPLE = 676,
     TABLESPACE = 677,
     TEMP = 678,
     TEMPLATE = 679,
     TEMPORARY = 680,
     TEXT_P = 681,
     THEN = 682,
     TIES = 683,
     TIME = 684,
     TIMESTAMP = 685,
     TO = 686,
     TRAILING = 687,
     TRANSACTION = 688,
     TRANSFORM = 689,
     TREAT = 690,
     TRIGGER = 691,
     TRIM = 692,
     TRUE_P = 693,
     TRUNCATE = 694,
     TRUSTED = 695,
     TRY_CAST = 696,
     TYPE_P = 697,
     TYPES_P = 698,
     UNBOUNDED = 699,
     UNCOMMITTED = 700,
     UNENCRYPTED = 701,
     UNION = 702,
     UNIQUE = 703,
     UNKNOWN = 704,
     UNLISTEN = 705,
     UNLOGGED = 706,
     UNPIVOT = 707,
     UNTIL = 708,
     UPDATE = 709,
     USE_P = 710,
     USER = 711,
     USING = 712,
     VACUUM = 713,
     VALID = 714,
     VALIDATE = 715,
     VALIDATOR = 716,
     VALUE_P = 717,
     VALUES = 718,
     VARCHAR = 719,
     VARIABLE_P = 720,
     VARIADIC = 721,
     VARYING = 722,
     VERBOSE = 723,
     VERSION_P = 724,
     VIEW = 725,
     VIEWS = 726,
     VIRTUAL = 727,
     VOLATILE = 728,
     WEEK_P = 729,
     WEEKS_P = 730,
     WHEN = 731,
     WHERE = 732,
     WHITESPACE_P = 733,
     WINDOW = 734,
     WITH = 735,
     WITHIN = 736,
     WITHOUT = 737,
     WORK = 738,
     WRAPPER = 739,
     WRITE_P = 740,
     XML_P = 741,
     XMLATTRIBUTES = 742,
     XMLCONCAT = 743,
     XMLELEMENT = 744,
     XMLEXISTS = 745,
     XMLFOREST = 746,
     XMLNAMESPACES = 747,
     XMLPARSE = 748,
     XMLPI = 749,
     XMLROOT = 750,
     XMLSERIALIZE = 751,
     XMLTABLE = 752,
     YEAR_P = 753,
     YEARS_P = 754,
     YES_P = 755,
     ZONE = 756,
     NOT_LA = 757,
     NULLS_LA = 758,
     WITH_LA = 759,
     POSTFIXOP = 760,
     UMINUS = 761
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
#define EXTENSIONS 412
#define EXTERNAL 413
#define EXTRACT 414
#define FALSE_P 415
#define FAMILY 416
#define FETCH 417
#define FILTER 418
#define FIRST_P 419
#define FLOAT_P 420
#define FOLLOWING 421
#define FOR 422
#define FORCE 423
#define FOREIGN 424
#define FORWARD 425
#define FREEZE 426
#define FROM 427
#define FULL 428
#define FUNCTION 429
#define FUNCTIONS 430
#define GENERATED 431
#define GLOB 432
#define GLOBAL 433
#define GRANT 434
#define GRANTED 435
#define GROUP_P 436
#define GROUPING 437
#define GROUPING_ID 438
#define GROUPS 439
#define HANDLER 440
#define HAVING 441
#define HEADER_P 442
#define HOLD 443
#define HOUR_P 444
#define HOURS_P 445
#define IDENTITY_P 446
#define IF_P 447
#define IGNORE_P 448
#define ILIKE 449
#define IMMEDIATE 450
#define IMMUTABLE 451
#define IMPLICIT_P 452
#define IMPORT_P 453
#define IN_P 454
#define INCLUDE_P 455
#define INCLUDING 456
#define INCREMENT 457
#define INDEX 458
#define INDEXES 459
#define INHERIT 460
#define INHERITS 461
#define INITIALLY 462
#define INLINE_P 463
#define INNER_P 464
#define INOUT 465
#define INPUT_P 466
#define INSENSITIVE 467
#define INSERT 468
#define INSTALL 469
#define INSTEAD 470
#define INT_P 471
#define INTEGER 472
#define INTERSECT 473
#define INTERVAL 474
#define INTO 475
#define INVOKER 476
#define IS 477
#define ISNULL 478
#define ISOLATION 479
#define JOIN 480
#define JSON 481
#define KEY 482
#define LABEL 483
#define LANGUAGE 484
#define LARGE_P 485
#define LAST_P 486
#define LATERAL_P 487
#define LEADING 488
#define LEAKPROOF 489
#define LEFT 490
#define LEVEL 491
#define LIKE 492
#define LIMIT 493
#define LISTEN 494
#define LOAD 495
#define LOCAL 496
#define LOCATION 497
#define LOCK_P 498
#define LOCKED 499
#define LOGGED 500
#define MACRO 501
#define MAP 502
#define MAPPING 503
#define MATCH 504
#define MATERIALIZED 505
#define MAXVALUE 506
#define METHOD 507
#define MICROSECOND_P 508
#define MICROSECONDS_P 509
#define MILLENNIA_P 510
#define MILLENNIUM_P 511
#define MILLISECOND_P 512
#define MILLISECONDS_P 513
#define MINUTE_P 514
#define MINUTES_P 515
#define MINVALUE 516
#define MODE 517
#define MONTH_P 518
#define MONTHS_P 519
#define MOVE 520
#define NAME_P 521
#define NAMES 522
#define NATIONAL 523
#define NATURAL 524
#define NCHAR 525
#define NEW 526
#define NEXT 527
#define NO 528
#define NONE 529
#define NOT 530
#define NOTHING 531
#define NOTIFY 532
#define NOTNULL 533
#define NOWAIT 534
#define NULL_P 535
#define NULLIF 536
#define NULLS_P 537
#define NUMERIC 538
#define OBJECT_P 539
#define OF 540
#define OFF 541
#define OFFSET 542
#define OIDS 543
#define OLD 544
#define ON 545
#define ONLY 546
#define OPERATOR 547
#define OPTION 548
#define OPTIONS 549
#define OR 550
#define ORDER 551
#define ORDINALITY 552
#define OTHERS 553
#define OUT_P 554
#define OUTER_P 555
#define OVER 556
#define OVERLAPS 557
#define OVERLAY 558
#define OVERRIDING 559
#define OWNED 560
#define OWNER 561
#define PARALLEL 562
#define PARSER 563
#define PARTIAL 564
#define PARTITION 565
#define PASSING 566
#define PASSWORD 567
#define PERCENT 568
#define PERSISTENT 569
#define PIVOT 570
#define PIVOT_LONGER 571
#define PIVOT_WIDER 572
#define PLACING 573
#define PLANS 574
#define POLICY 575
#define POSITION 576
#define POSITIONAL 577
#define PRAGMA_P 578
#define PRECEDING 579
#define PRECISION 580
#define PREPARE 581
#define PREPARED 582
#define PRESERVE 583
#define PRIMARY 584
#define PRIOR 585
#define PRIVILEGES 586
#define PROCEDURAL 587
#define PROCEDURE 588
#define PROGRAM 589
#define PUBLICATION 590
#define QUALIFY 591
#define QUARTER_P 592
#define QUARTERS_P 593
#define QUOTE 594
#define RANGE 595
#define READ_P 596
#define REAL 597
#define REASSIGN 598
#define RECHECK 599
#define RECURSIVE 600
#define REF 601
#define REFERENCES 602
#define REFERENCING 603
#define REFRESH 604
#define REINDEX 605
#define RELATIVE_P 606
#define RELEASE 607
#define RENAME 608
#define REPEATABLE 609
#define REPLACE 610
#define REPLICA 611
#define RESET 612
#define RESPECT_P 613
#define RESTART 614
#define RESTRICT 615
#define RETURNING 616
#define RETURNS 617
#define REVOKE 618
#define RIGHT 619
#define ROLE 620
#define ROLLBACK 621
#define ROLLUP 622
#define ROW 623
#define ROWS 624
#define RULE 625
#define SAMPLE 626
#define SAVEPOINT 627
#define SCHEMA 628
#define SCHEMAS 629
#define SCOPE 630
#define SCROLL 631
#define SEARCH 632
#define SECOND_P 633
#define SECONDS_P 634
#define SECRET 635
#define SECURITY 636
#define SELECT 637
#define SEMI 638
#define SEQUENCE 639
#define SEQUENCES 640
#define SERIALIZABLE 641
#define SERVER 642
#define SESSION 643
#define SET 644
#define SETOF 645
#define SETS 646
#define SHARE 647
#define SHOW 648
#define SIMILAR 649
#define SIMPLE 650
#define SKIP 651
#define SMALLINT 652
#define SNAPSHOT 653
#define SOME 654
#define SQL_P 655
#define STABLE 656
#define STANDALONE_P 657
#define START 658
#define STATEMENT 659
#define STATISTICS 660
#define STDIN 661
#define STDOUT 662
#define STORAGE 663
#define STORED 664
#define STRICT_P 665
#define STRIP_P 666
#define STRUCT 667
#define SUBSCRIPTION 668
#define SUBSTRING 669
#define SUMMARIZE 670
#define SYMMETRIC 671
#define SYSID 672
#define SYSTEM_P 673
#define TABLE 674
#define TABLES 675
#define TABLESAMPLE 676
#define TABLESPACE 677
#define TEMP 678
#define TEMPLATE 679
#define TEMPORARY 680
#define TEXT_P 681
#define THEN 682
#define TIES 683
#define TIME 684
#define TIMESTAMP 685
#define TO 686
#define TRAILING 687
#define TRANSACTION 688
#define TRANSFORM 689
#define TREAT 690
#define TRIGGER 691
#define TRIM 692
#define TRUE_P 693
#define TRUNCATE 694
#define TRUSTED 695
#define TRY_CAST 696
#define TYPE_P 697
#define TYPES_P 698
#define UNBOUNDED 699
#define UNCOMMITTED 700
#define UNENCRYPTED 701
#define UNION 702
#define UNIQUE 703
#define UNKNOWN 704
#define UNLISTEN 705
#define UNLOGGED 706
#define UNPIVOT 707
#define UNTIL 708
#define UPDATE 709
#define USE_P 710
#define USER 711
#define USING 712
#define VACUUM 713
#define VALID 714
#define VALIDATE 715
#define VALIDATOR 716
#define VALUE_P 717
#define VALUES 718
#define VARCHAR 719
#define VARIABLE_P 720
#define VARIADIC 721
#define VARYING 722
#define VERBOSE 723
#define VERSION_P 724
#define VIEW 725
#define VIEWS 726
#define VIRTUAL 727
#define VOLATILE 728
#define WEEK_P 729
#define WEEKS_P 730
#define WHEN 731
#define WHERE 732
#define WHITESPACE_P 733
#define WINDOW 734
#define WITH 735
#define WITHIN 736
#define WITHOUT 737
#define WORK 738
#define WRAPPER 739
#define WRITE_P 740
#define XML_P 741
#define XMLATTRIBUTES 742
#define XMLCONCAT 743
#define XMLELEMENT 744
#define XMLEXISTS 745
#define XMLFOREST 746
#define XMLNAMESPACES 747
#define XMLPARSE 748
#define XMLPI 749
#define XMLROOT 750
#define XMLSERIALIZE 751
#define XMLTABLE 752
#define YEAR_P 753
#define YEARS_P 754
#define YES_P 755
#define ZONE 756
#define NOT_LA 757
#define NULLS_LA 758
#define WITH_LA 759
#define POSTFIXOP 760
#define UMINUS 761




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
	PGLoadInstallType loadinstalltype;
	PGTransactionStmtType transactiontype;
}
/* Line 1529 of yacc.c.  */
#line 1112 "third_party/libpg_query/grammar/grammar_out.hpp"
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


