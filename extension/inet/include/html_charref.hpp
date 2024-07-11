//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/extension/inet/include/html_charref.hpp
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/common/string_map_set.hpp"
namespace duckdb {

// see https://html.spec.whatwg.org/multipage/parsing.html#numeric-character-reference-end-state
const unordered_map<uint32_t, const char *> ReturnInvalidCharrefs() {
	return {
	    {0x80, "\u20ac"}, // EURO SIGN
	    {0x82, "\u201a"}, // SINGLE LOW-9 QUOTATION MARK
	    {0x83, "\u0192"}, // LATIN SMALL LETTER F WITH HOOK
	    {0x84, "\u201e"}, // DOUBLE LOW-9 QUOTATION MARK
	    {0x85, "\u2026"}, // HORIZONTAL ELLIPSIS
	    {0x86, "\u2020"}, // DAGGER
	    {0x87, "\u2021"}, // DOUBLE DAGGER
	    {0x88, "\u02c6"}, // MODIFIER LETTER CIRCUMFLEX ACCENT
	    {0x89, "\u2030"}, // PER MILLE SIGN
	    {0x8a, "\u0160"}, // LATIN CAPITAL LETTER S WITH CARON
	    {0x8b, "\u2039"}, // SINGLE LEFT-POINTING ANGLE QUOTATION MARK
	    {0x8c, "\u0152"}, // LATIN CAPITAL LIGATURE OE
	    {0x8e, "\u017d"}, // LATIN CAPITAL LETTER Z WITH CARON
	    {0x91, "\u2018"}, // LEFT SINGLE QUOTATION MARK
	    {0x92, "\u2019"}, // RIGHT SINGLE QUOTATION MARK
	    {0x93, "\u201c"}, // LEFT DOUBLE QUOTATION MARK
	    {0x94, "\u201d"}, // RIGHT DOUBLE QUOTATION MARK
	    {0x95, "\u2022"}, // BULLET
	    {0x96, "\u2013"}, // EN DASH
	    {0x97, "\u2014"}, // EM DASH
	    {0x98, "\u02dc"}, // SMALL TILDE
	    {0x99, "\u2122"}, // TRADE MARK SIGN
	    {0x9a, "\u0161"}, // LATIN SMALL LETTER S WITH CARON
	    {0x9b, "\u203a"}, // SINGLE RIGHT-POINTING ANGLE QUOTATION MARK
	    {0x9c, "\u0153"}, // LATIN SMALL LIGATURE OE
	    {0x9e, "\u017e"}, // LATIN SMALL LETTER Z WITH CARON
	    {0x9f, "\u0178"}  // LATIN CAPITAL LETTER Y WITH DIAERESIS
	};
}

// list of non-printable code points
const unordered_set<uint32_t> ReturnInvalidCodepoints() {
	return {// 0x0001 to 0x0008 - control characters
	        0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8,
	        // 0x000E to 0x001F - control characters
	        0xe, 0xf, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	        // 0x7f - the 'delete' control character
	        0x7f,
	        // 0xFDD0 to 0xFDEF - code points reserved for internal use
	        0xfdd0, 0xfdd1, 0xfdd2, 0xfdd3, 0xfdd4, 0xfdd5, 0xfdd6, 0xfdd7, 0xfdd8, 0xfdd9, 0xfdda, 0xfddb, 0xfddc,
	        0xfddd, 0xfdde, 0xfddf, 0xfde0, 0xfde1, 0xfde2, 0xfde3, 0xfde4, 0xfde5, 0xfde6, 0xfde7, 0xfde8, 0xfde9,
	        0xfdea, 0xfdeb, 0xfdec, 0xfded, 0xfdee, 0xfdef,
	        // others - surrogate code points
	        0xb, 0xfffe, 0xffff, 0x1fffe, 0x1ffff, 0x2fffe, 0x2ffff, 0x3fffe, 0x3ffff, 0x4fffe, 0x4ffff, 0x5fffe,
	        0x5ffff, 0x6fffe, 0x6ffff, 0x7fffe, 0x7ffff, 0x8fffe, 0x8ffff, 0x9fffe, 0x9ffff, 0xafffe, 0xaffff, 0xbfffe,
	        0xbffff, 0xcfffe, 0xcffff, 0xdfffe, 0xdffff, 0xefffe, 0xeffff, 0xffffe, 0xfffff, 0x10fffe, 0x10ffff};
}

// some glyphs are the combination of two codepoints
struct HTMLEscapeCodepoint {
	// if there is only one codepoint, codepoints[1] is equal to zero
	uint32_t codepoints[2];
};

// define the equality and inequality operators between HTMLEscapeCodepoint objects
bool operator==(const HTMLEscapeCodepoint &lhs, const HTMLEscapeCodepoint &rhs) {
	return lhs.codepoints[0] == rhs.codepoints[0] && lhs.codepoints[1] == rhs.codepoints[1];
}

bool operator!=(const HTMLEscapeCodepoint &lhs, const HTMLEscapeCodepoint &rhs) {
	return !(lhs == rhs);
}

// HTML5 named character references
// see https://html.spec.whatwg.org/multipage/named-characters.html.
// This maps HTML5 named character references to the equivalent Unicode character(s).
// copied from https://html.spec.whatwg.org/entities.json
struct HTML5NameCharrefs {
	static const string_map_t<HTMLEscapeCodepoint> mapped_strings;

private:
	static vector<string> initialize_vector() {
		return {
		    "AElig",                            // 0
		    "AElig;",                           // 1
		    "AMP",                              // 2
		    "AMP;",                             // 3
		    "Aacute",                           // 4
		    "Aacute;",                          // 5
		    "Abreve;",                          // 6
		    "Acirc",                            // 7
		    "Acirc;",                           // 8
		    "Acy;",                             // 9
		    "Afr;",                             // 10
		    "Agrave",                           // 11
		    "Agrave;",                          // 12
		    "Alpha;",                           // 13
		    "Amacr;",                           // 14
		    "And;",                             // 15
		    "Aogon;",                           // 16
		    "Aopf;",                            // 17
		    "ApplyFunction;",                   // 18
		    "Aring",                            // 19
		    "Aring;",                           // 20
		    "Ascr;",                            // 21
		    "Assign;",                          // 22
		    "Atilde",                           // 23
		    "Atilde;",                          // 24
		    "Auml",                             // 25
		    "Auml;",                            // 26
		    "Backslash;",                       // 27
		    "Barv;",                            // 28
		    "Barwed;",                          // 29
		    "Bcy;",                             // 30
		    "Because;",                         // 31
		    "Bernoullis;",                      // 32
		    "Beta;",                            // 33
		    "Bfr;",                             // 34
		    "Bopf;",                            // 35
		    "Breve;",                           // 36
		    "Bscr;",                            // 37
		    "Bumpeq;",                          // 38
		    "CHcy;",                            // 39
		    "COPY",                             // 40
		    "COPY;",                            // 41
		    "Cacute;",                          // 42
		    "Cap;",                             // 43
		    "CapitalDifferentialD;",            // 44
		    "Cayleys;",                         // 45
		    "Ccaron;",                          // 46
		    "Ccedil",                           // 47
		    "Ccedil;",                          // 48
		    "Ccirc;",                           // 49
		    "Cconint;",                         // 50
		    "Cdot;",                            // 51
		    "Cedilla;",                         // 52
		    "CenterDot;",                       // 53
		    "Cfr;",                             // 54
		    "Chi;",                             // 55
		    "CircleDot;",                       // 56
		    "CircleMinus;",                     // 57
		    "CirclePlus;",                      // 58
		    "CircleTimes;",                     // 59
		    "ClockwiseContourIntegral;",        // 60
		    "CloseCurlyDoubleQuote;",           // 61
		    "CloseCurlyQuote;",                 // 62
		    "Colon;",                           // 63
		    "Colone;",                          // 64
		    "Congruent;",                       // 65
		    "Conint;",                          // 66
		    "ContourIntegral;",                 // 67
		    "Copf;",                            // 68
		    "Coproduct;",                       // 69
		    "CounterClockwiseContourIntegral;", // 70
		    "Cross;",                           // 71
		    "Cscr;",                            // 72
		    "Cup;",                             // 73
		    "CupCap;",                          // 74
		    "DD;",                              // 75
		    "DDotrahd;",                        // 76
		    "DJcy;",                            // 77
		    "DScy;",                            // 78
		    "DZcy;",                            // 79
		    "Dagger;",                          // 80
		    "Darr;",                            // 81
		    "Dashv;",                           // 82
		    "Dcaron;",                          // 83
		    "Dcy;",                             // 84
		    "Del;",                             // 85
		    "Delta;",                           // 86
		    "Dfr;",                             // 87
		    "DiacriticalAcute;",                // 88
		    "DiacriticalDot;",                  // 89
		    "DiacriticalDoubleAcute;",          // 90
		    "DiacriticalGrave;",                // 91
		    "DiacriticalTilde;",                // 92
		    "Diamond;",                         // 93
		    "DifferentialD;",                   // 94
		    "Dopf;",                            // 95
		    "Dot;",                             // 96
		    "DotDot;",                          // 97
		    "DotEqual;",                        // 98
		    "DoubleContourIntegral;",           // 99
		    "DoubleDot;",                       // 100
		    "DoubleDownArrow;",                 // 101
		    "DoubleLeftArrow;",                 // 102
		    "DoubleLeftRightArrow;",            // 103
		    "DoubleLeftTee;",                   // 104
		    "DoubleLongLeftArrow;",             // 105
		    "DoubleLongLeftRightArrow;",        // 106
		    "DoubleLongRightArrow;",            // 107
		    "DoubleRightArrow;",                // 108
		    "DoubleRightTee;",                  // 109
		    "DoubleUpArrow;",                   // 110
		    "DoubleUpDownArrow;",               // 111
		    "DoubleVerticalBar;",               // 112
		    "DownArrow;",                       // 113
		    "DownArrowBar;",                    // 114
		    "DownArrowUpArrow;",                // 115
		    "DownBreve;",                       // 116
		    "DownLeftRightVector;",             // 117
		    "DownLeftTeeVector;",               // 118
		    "DownLeftVector;",                  // 119
		    "DownLeftVectorBar;",               // 120
		    "DownRightTeeVector;",              // 121
		    "DownRightVector;",                 // 122
		    "DownRightVectorBar;",              // 123
		    "DownTee;",                         // 124
		    "DownTeeArrow;",                    // 125
		    "Downarrow;",                       // 126
		    "Dscr;",                            // 127
		    "Dstrok;",                          // 128
		    "ENG;",                             // 129
		    "ETH",                              // 130
		    "ETH;",                             // 131
		    "Eacute",                           // 132
		    "Eacute;",                          // 133
		    "Ecaron;",                          // 134
		    "Ecirc",                            // 135
		    "Ecirc;",                           // 136
		    "Ecy;",                             // 137
		    "Edot;",                            // 138
		    "Efr;",                             // 139
		    "Egrave",                           // 140
		    "Egrave;",                          // 141
		    "Element;",                         // 142
		    "Emacr;",                           // 143
		    "EmptySmallSquare;",                // 144
		    "EmptyVerySmallSquare;",            // 145
		    "Eogon;",                           // 146
		    "Eopf;",                            // 147
		    "Epsilon;",                         // 148
		    "Equal;",                           // 149
		    "EqualTilde;",                      // 150
		    "Equilibrium;",                     // 151
		    "Escr;",                            // 152
		    "Esim;",                            // 153
		    "Eta;",                             // 154
		    "Euml",                             // 155
		    "Euml;",                            // 156
		    "Exists;",                          // 157
		    "ExponentialE;",                    // 158
		    "Fcy;",                             // 159
		    "Ffr;",                             // 160
		    "FilledSmallSquare;",               // 161
		    "FilledVerySmallSquare;",           // 162
		    "Fopf;",                            // 163
		    "ForAll;",                          // 164
		    "Fouriertrf;",                      // 165
		    "Fscr;",                            // 166
		    "GJcy;",                            // 167
		    "GT",                               // 168
		    "GT;",                              // 169
		    "Gamma;",                           // 170
		    "Gammad;",                          // 171
		    "Gbreve;",                          // 172
		    "Gcedil;",                          // 173
		    "Gcirc;",                           // 174
		    "Gcy;",                             // 175
		    "Gdot;",                            // 176
		    "Gfr;",                             // 177
		    "Gg;",                              // 178
		    "Gopf;",                            // 179
		    "GreaterEqual;",                    // 180
		    "GreaterEqualLess;",                // 181
		    "GreaterFullEqual;",                // 182
		    "GreaterGreater;",                  // 183
		    "GreaterLess;",                     // 184
		    "GreaterSlantEqual;",               // 185
		    "GreaterTilde;",                    // 186
		    "Gscr;",                            // 187
		    "Gt;",                              // 188
		    "HARDcy;",                          // 189
		    "Hacek;",                           // 190
		    "Hat;",                             // 191
		    "Hcirc;",                           // 192
		    "Hfr;",                             // 193
		    "HilbertSpace;",                    // 194
		    "Hopf;",                            // 195
		    "HorizontalLine;",                  // 196
		    "Hscr;",                            // 197
		    "Hstrok;",                          // 198
		    "HumpDownHump;",                    // 199
		    "HumpEqual;",                       // 200
		    "IEcy;",                            // 201
		    "IJlig;",                           // 202
		    "IOcy;",                            // 203
		    "Iacute",                           // 204
		    "Iacute;",                          // 205
		    "Icirc",                            // 206
		    "Icirc;",                           // 207
		    "Icy;",                             // 208
		    "Idot;",                            // 209
		    "Ifr;",                             // 210
		    "Igrave",                           // 211
		    "Igrave;",                          // 212
		    "Im;",                              // 213
		    "Imacr;",                           // 214
		    "ImaginaryI;",                      // 215
		    "Implies;",                         // 216
		    "Int;",                             // 217
		    "Integral;",                        // 218
		    "Intersection;",                    // 219
		    "InvisibleComma;",                  // 220
		    "InvisibleTimes;",                  // 221
		    "Iogon;",                           // 222
		    "Iopf;",                            // 223
		    "Iota;",                            // 224
		    "Iscr;",                            // 225
		    "Itilde;",                          // 226
		    "Iukcy;",                           // 227
		    "Iuml",                             // 228
		    "Iuml;",                            // 229
		    "Jcirc;",                           // 230
		    "Jcy;",                             // 231
		    "Jfr;",                             // 232
		    "Jopf;",                            // 233
		    "Jscr;",                            // 234
		    "Jsercy;",                          // 235
		    "Jukcy;",                           // 236
		    "KHcy;",                            // 237
		    "KJcy;",                            // 238
		    "Kappa;",                           // 239
		    "Kcedil;",                          // 240
		    "Kcy;",                             // 241
		    "Kfr;",                             // 242
		    "Kopf;",                            // 243
		    "Kscr;",                            // 244
		    "LJcy;",                            // 245
		    "LT",                               // 246
		    "LT;",                              // 247
		    "Lacute;",                          // 248
		    "Lambda;",                          // 249
		    "Lang;",                            // 250
		    "Laplacetrf;",                      // 251
		    "Larr;",                            // 252
		    "Lcaron;",                          // 253
		    "Lcedil;",                          // 254
		    "Lcy;",                             // 255
		    "LeftAngleBracket;",                // 256
		    "LeftArrow;",                       // 257
		    "LeftArrowBar;",                    // 258
		    "LeftArrowRightArrow;",             // 259
		    "LeftCeiling;",                     // 260
		    "LeftDoubleBracket;",               // 261
		    "LeftDownTeeVector;",               // 262
		    "LeftDownVector;",                  // 263
		    "LeftDownVectorBar;",               // 264
		    "LeftFloor;",                       // 265
		    "LeftRightArrow;",                  // 266
		    "LeftRightVector;",                 // 267
		    "LeftTee;",                         // 268
		    "LeftTeeArrow;",                    // 269
		    "LeftTeeVector;",                   // 270
		    "LeftTriangle;",                    // 271
		    "LeftTriangleBar;",                 // 272
		    "LeftTriangleEqual;",               // 273
		    "LeftUpDownVector;",                // 274
		    "LeftUpTeeVector;",                 // 275
		    "LeftUpVector;",                    // 276
		    "LeftUpVectorBar;",                 // 277
		    "LeftVector;",                      // 278
		    "LeftVectorBar;",                   // 279
		    "Leftarrow;",                       // 280
		    "Leftrightarrow;",                  // 281
		    "LessEqualGreater;",                // 282
		    "LessFullEqual;",                   // 283
		    "LessGreater;",                     // 284
		    "LessLess;",                        // 285
		    "LessSlantEqual;",                  // 286
		    "LessTilde;",                       // 287
		    "Lfr;",                             // 288
		    "Ll;",                              // 289
		    "Lleftarrow;",                      // 290
		    "Lmidot;",                          // 291
		    "LongLeftArrow;",                   // 292
		    "LongLeftRightArrow;",              // 293
		    "LongRightArrow;",                  // 294
		    "Longleftarrow;",                   // 295
		    "Longleftrightarrow;",              // 296
		    "Longrightarrow;",                  // 297
		    "Lopf;",                            // 298
		    "LowerLeftArrow;",                  // 299
		    "LowerRightArrow;",                 // 300
		    "Lscr;",                            // 301
		    "Lsh;",                             // 302
		    "Lstrok;",                          // 303
		    "Lt;",                              // 304
		    "Map;",                             // 305
		    "Mcy;",                             // 306
		    "MediumSpace;",                     // 307
		    "Mellintrf;",                       // 308
		    "Mfr;",                             // 309
		    "MinusPlus;",                       // 310
		    "Mopf;",                            // 311
		    "Mscr;",                            // 312
		    "Mu;",                              // 313
		    "NJcy;",                            // 314
		    "Nacute;",                          // 315
		    "Ncaron;",                          // 316
		    "Ncedil;",                          // 317
		    "Ncy;",                             // 318
		    "NegativeMediumSpace;",             // 319
		    "NegativeThickSpace;",              // 320
		    "NegativeThinSpace;",               // 321
		    "NegativeVeryThinSpace;",           // 322
		    "NestedGreaterGreater;",            // 323
		    "NestedLessLess;",                  // 324
		    "NewLine;",                         // 325
		    "Nfr;",                             // 326
		    "NoBreak;",                         // 327
		    "NonBreakingSpace;",                // 328
		    "Nopf;",                            // 329
		    "Not;",                             // 330
		    "NotCongruent;",                    // 331
		    "NotCupCap;",                       // 332
		    "NotDoubleVerticalBar;",            // 333
		    "NotElement;",                      // 334
		    "NotEqual;",                        // 335
		    "NotEqualTilde;",                   // 336
		    "NotExists;",                       // 337
		    "NotGreater;",                      // 338
		    "NotGreaterEqual;",                 // 339
		    "NotGreaterFullEqual;",             // 340
		    "NotGreaterGreater;",               // 341
		    "NotGreaterLess;",                  // 342
		    "NotGreaterSlantEqual;",            // 343
		    "NotGreaterTilde;",                 // 344
		    "NotHumpDownHump;",                 // 345
		    "NotHumpEqual;",                    // 346
		    "NotLeftTriangle;",                 // 347
		    "NotLeftTriangleBar;",              // 348
		    "NotLeftTriangleEqual;",            // 349
		    "NotLess;",                         // 350
		    "NotLessEqual;",                    // 351
		    "NotLessGreater;",                  // 352
		    "NotLessLess;",                     // 353
		    "NotLessSlantEqual;",               // 354
		    "NotLessTilde;",                    // 355
		    "NotNestedGreaterGreater;",         // 356
		    "NotNestedLessLess;",               // 357
		    "NotPrecedes;",                     // 358
		    "NotPrecedesEqual;",                // 359
		    "NotPrecedesSlantEqual;",           // 360
		    "NotReverseElement;",               // 361
		    "NotRightTriangle;",                // 362
		    "NotRightTriangleBar;",             // 363
		    "NotRightTriangleEqual;",           // 364
		    "NotSquareSubset;",                 // 365
		    "NotSquareSubsetEqual;",            // 366
		    "NotSquareSuperset;",               // 367
		    "NotSquareSupersetEqual;",          // 368
		    "NotSubset;",                       // 369
		    "NotSubsetEqual;",                  // 370
		    "NotSucceeds;",                     // 371
		    "NotSucceedsEqual;",                // 372
		    "NotSucceedsSlantEqual;",           // 373
		    "NotSucceedsTilde;",                // 374
		    "NotSuperset;",                     // 375
		    "NotSupersetEqual;",                // 376
		    "NotTilde;",                        // 377
		    "NotTildeEqual;",                   // 378
		    "NotTildeFullEqual;",               // 379
		    "NotTildeTilde;",                   // 380
		    "NotVerticalBar;",                  // 381
		    "Nscr;",                            // 382
		    "Ntilde",                           // 383
		    "Ntilde;",                          // 384
		    "Nu;",                              // 385
		    "OElig;",                           // 386
		    "Oacute",                           // 387
		    "Oacute;",                          // 388
		    "Ocirc",                            // 389
		    "Ocirc;",                           // 390
		    "Ocy;",                             // 391
		    "Odblac;",                          // 392
		    "Ofr;",                             // 393
		    "Ograve",                           // 394
		    "Ograve;",                          // 395
		    "Omacr;",                           // 396
		    "Omega;",                           // 397
		    "Omicron;",                         // 398
		    "Oopf;",                            // 399
		    "OpenCurlyDoubleQuote;",            // 400
		    "OpenCurlyQuote;",                  // 401
		    "Or;",                              // 402
		    "Oscr;",                            // 403
		    "Oslash",                           // 404
		    "Oslash;",                          // 405
		    "Otilde",                           // 406
		    "Otilde;",                          // 407
		    "Otimes;",                          // 408
		    "Ouml",                             // 409
		    "Ouml;",                            // 410
		    "OverBar;",                         // 411
		    "OverBrace;",                       // 412
		    "OverBracket;",                     // 413
		    "OverParenthesis;",                 // 414
		    "PartialD;",                        // 415
		    "Pcy;",                             // 416
		    "Pfr;",                             // 417
		    "Phi;",                             // 418
		    "Pi;",                              // 419
		    "PlusMinus;",                       // 420
		    "Poincareplane;",                   // 421
		    "Popf;",                            // 422
		    "Pr;",                              // 423
		    "Precedes;",                        // 424
		    "PrecedesEqual;",                   // 425
		    "PrecedesSlantEqual;",              // 426
		    "PrecedesTilde;",                   // 427
		    "Prime;",                           // 428
		    "Product;",                         // 429
		    "Proportion;",                      // 430
		    "Proportional;",                    // 431
		    "Pscr;",                            // 432
		    "Psi;",                             // 433
		    "QUOT",                             // 434
		    "QUOT;",                            // 435
		    "Qfr;",                             // 436
		    "Qopf;",                            // 437
		    "Qscr;",                            // 438
		    "RBarr;",                           // 439
		    "REG",                              // 440
		    "REG;",                             // 441
		    "Racute;",                          // 442
		    "Rang;",                            // 443
		    "Rarr;",                            // 444
		    "Rarrtl;",                          // 445
		    "Rcaron;",                          // 446
		    "Rcedil;",                          // 447
		    "Rcy;",                             // 448
		    "Re;",                              // 449
		    "ReverseElement;",                  // 450
		    "ReverseEquilibrium;",              // 451
		    "ReverseUpEquilibrium;",            // 452
		    "Rfr;",                             // 453
		    "Rho;",                             // 454
		    "RightAngleBracket;",               // 455
		    "RightArrow;",                      // 456
		    "RightArrowBar;",                   // 457
		    "RightArrowLeftArrow;",             // 458
		    "RightCeiling;",                    // 459
		    "RightDoubleBracket;",              // 460
		    "RightDownTeeVector;",              // 461
		    "RightDownVector;",                 // 462
		    "RightDownVectorBar;",              // 463
		    "RightFloor;",                      // 464
		    "RightTee;",                        // 465
		    "RightTeeArrow;",                   // 466
		    "RightTeeVector;",                  // 467
		    "RightTriangle;",                   // 468
		    "RightTriangleBar;",                // 469
		    "RightTriangleEqual;",              // 470
		    "RightUpDownVector;",               // 471
		    "RightUpTeeVector;",                // 472
		    "RightUpVector;",                   // 473
		    "RightUpVectorBar;",                // 474
		    "RightVector;",                     // 475
		    "RightVectorBar;",                  // 476
		    "Rightarrow;",                      // 477
		    "Ropf;",                            // 478
		    "RoundImplies;",                    // 479
		    "Rrightarrow;",                     // 480
		    "Rscr;",                            // 481
		    "Rsh;",                             // 482
		    "RuleDelayed;",                     // 483
		    "SHCHcy;",                          // 484
		    "SHcy;",                            // 485
		    "SOFTcy;",                          // 486
		    "Sacute;",                          // 487
		    "Sc;",                              // 488
		    "Scaron;",                          // 489
		    "Scedil;",                          // 490
		    "Scirc;",                           // 491
		    "Scy;",                             // 492
		    "Sfr;",                             // 493
		    "ShortDownArrow;",                  // 494
		    "ShortLeftArrow;",                  // 495
		    "ShortRightArrow;",                 // 496
		    "ShortUpArrow;",                    // 497
		    "Sigma;",                           // 498
		    "SmallCircle;",                     // 499
		    "Sopf;",                            // 500
		    "Sqrt;",                            // 501
		    "Square;",                          // 502
		    "SquareIntersection;",              // 503
		    "SquareSubset;",                    // 504
		    "SquareSubsetEqual;",               // 505
		    "SquareSuperset;",                  // 506
		    "SquareSupersetEqual;",             // 507
		    "SquareUnion;",                     // 508
		    "Sscr;",                            // 509
		    "Star;",                            // 510
		    "Sub;",                             // 511
		    "Subset;",                          // 512
		    "SubsetEqual;",                     // 513
		    "Succeeds;",                        // 514
		    "SucceedsEqual;",                   // 515
		    "SucceedsSlantEqual;",              // 516
		    "SucceedsTilde;",                   // 517
		    "SuchThat;",                        // 518
		    "Sum;",                             // 519
		    "Sup;",                             // 520
		    "Superset;",                        // 521
		    "SupersetEqual;",                   // 522
		    "Supset;",                          // 523
		    "THORN",                            // 524
		    "THORN;",                           // 525
		    "TRADE;",                           // 526
		    "TSHcy;",                           // 527
		    "TScy;",                            // 528
		    "Tab;",                             // 529
		    "Tau;",                             // 530
		    "Tcaron;",                          // 531
		    "Tcedil;",                          // 532
		    "Tcy;",                             // 533
		    "Tfr;",                             // 534
		    "Therefore;",                       // 535
		    "Theta;",                           // 536
		    "ThickSpace;",                      // 537
		    "ThinSpace;",                       // 538
		    "Tilde;",                           // 539
		    "TildeEqual;",                      // 540
		    "TildeFullEqual;",                  // 541
		    "TildeTilde;",                      // 542
		    "Topf;",                            // 543
		    "TripleDot;",                       // 544
		    "Tscr;",                            // 545
		    "Tstrok;",                          // 546
		    "Uacute",                           // 547
		    "Uacute;",                          // 548
		    "Uarr;",                            // 549
		    "Uarrocir;",                        // 550
		    "Ubrcy;",                           // 551
		    "Ubreve;",                          // 552
		    "Ucirc",                            // 553
		    "Ucirc;",                           // 554
		    "Ucy;",                             // 555
		    "Udblac;",                          // 556
		    "Ufr;",                             // 557
		    "Ugrave",                           // 558
		    "Ugrave;",                          // 559
		    "Umacr;",                           // 560
		    "UnderBar;",                        // 561
		    "UnderBrace;",                      // 562
		    "UnderBracket;",                    // 563
		    "UnderParenthesis;",                // 564
		    "Union;",                           // 565
		    "UnionPlus;",                       // 566
		    "Uogon;",                           // 567
		    "Uopf;",                            // 568
		    "UpArrow;",                         // 569
		    "UpArrowBar;",                      // 570
		    "UpArrowDownArrow;",                // 571
		    "UpDownArrow;",                     // 572
		    "UpEquilibrium;",                   // 573
		    "UpTee;",                           // 574
		    "UpTeeArrow;",                      // 575
		    "Uparrow;",                         // 576
		    "Updownarrow;",                     // 577
		    "UpperLeftArrow;",                  // 578
		    "UpperRightArrow;",                 // 579
		    "Upsi;",                            // 580
		    "Upsilon;",                         // 581
		    "Uring;",                           // 582
		    "Uscr;",                            // 583
		    "Utilde;",                          // 584
		    "Uuml",                             // 585
		    "Uuml;",                            // 586
		    "VDash;",                           // 587
		    "Vbar;",                            // 588
		    "Vcy;",                             // 589
		    "Vdash;",                           // 590
		    "Vdashl;",                          // 591
		    "Vee;",                             // 592
		    "Verbar;",                          // 593
		    "Vert;",                            // 594
		    "VerticalBar;",                     // 595
		    "VerticalLine;",                    // 596
		    "VerticalSeparator;",               // 597
		    "VerticalTilde;",                   // 598
		    "VeryThinSpace;",                   // 599
		    "Vfr;",                             // 600
		    "Vopf;",                            // 601
		    "Vscr;",                            // 602
		    "Vvdash;",                          // 603
		    "Wcirc;",                           // 604
		    "Wedge;",                           // 605
		    "Wfr;",                             // 606
		    "Wopf;",                            // 607
		    "Wscr;",                            // 608
		    "Xfr;",                             // 609
		    "Xi;",                              // 610
		    "Xopf;",                            // 611
		    "Xscr;",                            // 612
		    "YAcy;",                            // 613
		    "YIcy;",                            // 614
		    "YUcy;",                            // 615
		    "Yacute",                           // 616
		    "Yacute;",                          // 617
		    "Ycirc;",                           // 618
		    "Ycy;",                             // 619
		    "Yfr;",                             // 620
		    "Yopf;",                            // 621
		    "Yscr;",                            // 622
		    "Yuml;",                            // 623
		    "ZHcy;",                            // 624
		    "Zacute;",                          // 625
		    "Zcaron;",                          // 626
		    "Zcy;",                             // 627
		    "Zdot;",                            // 628
		    "ZeroWidthSpace;",                  // 629
		    "Zeta;",                            // 630
		    "Zfr;",                             // 631
		    "Zopf;",                            // 632
		    "Zscr;",                            // 633
		    "aacute",                           // 634
		    "aacute;",                          // 635
		    "abreve;",                          // 636
		    "ac;",                              // 637
		    "acE;",                             // 638
		    "acd;",                             // 639
		    "acirc",                            // 640
		    "acirc;",                           // 641
		    "acute",                            // 642
		    "acute;",                           // 643
		    "acy;",                             // 644
		    "aelig",                            // 645
		    "aelig;",                           // 646
		    "af;",                              // 647
		    "afr;",                             // 648
		    "agrave",                           // 649
		    "agrave;",                          // 650
		    "alefsym;",                         // 651
		    "aleph;",                           // 652
		    "alpha;",                           // 653
		    "amacr;",                           // 654
		    "amalg;",                           // 655
		    "amp",                              // 656
		    "amp;",                             // 657
		    "and;",                             // 658
		    "andand;",                          // 659
		    "andd;",                            // 660
		    "andslope;",                        // 661
		    "andv;",                            // 662
		    "ang;",                             // 663
		    "ange;",                            // 664
		    "angle;",                           // 665
		    "angmsd;",                          // 666
		    "angmsdaa;",                        // 667
		    "angmsdab;",                        // 668
		    "angmsdac;",                        // 669
		    "angmsdad;",                        // 670
		    "angmsdae;",                        // 671
		    "angmsdaf;",                        // 672
		    "angmsdag;",                        // 673
		    "angmsdah;",                        // 674
		    "angrt;",                           // 675
		    "angrtvb;",                         // 676
		    "angrtvbd;",                        // 677
		    "angsph;",                          // 678
		    "angst;",                           // 679
		    "angzarr;",                         // 680
		    "aogon;",                           // 681
		    "aopf;",                            // 682
		    "ap;",                              // 683
		    "apE;",                             // 684
		    "apacir;",                          // 685
		    "ape;",                             // 686
		    "apid;",                            // 687
		    "apos;",                            // 688
		    "approx;",                          // 689
		    "approxeq;",                        // 690
		    "aring",                            // 691
		    "aring;",                           // 692
		    "ascr;",                            // 693
		    "ast;",                             // 694
		    "asymp;",                           // 695
		    "asympeq;",                         // 696
		    "atilde",                           // 697
		    "atilde;",                          // 698
		    "auml",                             // 699
		    "auml;",                            // 700
		    "awconint;",                        // 701
		    "awint;",                           // 702
		    "bNot;",                            // 703
		    "backcong;",                        // 704
		    "backepsilon;",                     // 705
		    "backprime;",                       // 706
		    "backsim;",                         // 707
		    "backsimeq;",                       // 708
		    "barvee;",                          // 709
		    "barwed;",                          // 710
		    "barwedge;",                        // 711
		    "bbrk;",                            // 712
		    "bbrktbrk;",                        // 713
		    "bcong;",                           // 714
		    "bcy;",                             // 715
		    "bdquo;",                           // 716
		    "becaus;",                          // 717
		    "because;",                         // 718
		    "bemptyv;",                         // 719
		    "bepsi;",                           // 720
		    "bernou;",                          // 721
		    "beta;",                            // 722
		    "beth;",                            // 723
		    "between;",                         // 724
		    "bfr;",                             // 725
		    "bigcap;",                          // 726
		    "bigcirc;",                         // 727
		    "bigcup;",                          // 728
		    "bigodot;",                         // 729
		    "bigoplus;",                        // 730
		    "bigotimes;",                       // 731
		    "bigsqcup;",                        // 732
		    "bigstar;",                         // 733
		    "bigtriangledown;",                 // 734
		    "bigtriangleup;",                   // 735
		    "biguplus;",                        // 736
		    "bigvee;",                          // 737
		    "bigwedge;",                        // 738
		    "bkarow;",                          // 739
		    "blacklozenge;",                    // 740
		    "blacksquare;",                     // 741
		    "blacktriangle;",                   // 742
		    "blacktriangledown;",               // 743
		    "blacktriangleleft;",               // 744
		    "blacktriangleright;",              // 745
		    "blank;",                           // 746
		    "blk12;",                           // 747
		    "blk14;",                           // 748
		    "blk34;",                           // 749
		    "block;",                           // 750
		    "bne;",                             // 751
		    "bnequiv;",                         // 752
		    "bnot;",                            // 753
		    "bopf;",                            // 754
		    "bot;",                             // 755
		    "bottom;",                          // 756
		    "bowtie;",                          // 757
		    "boxDL;",                           // 758
		    "boxDR;",                           // 759
		    "boxDl;",                           // 760
		    "boxDr;",                           // 761
		    "boxH;",                            // 762
		    "boxHD;",                           // 763
		    "boxHU;",                           // 764
		    "boxHd;",                           // 765
		    "boxHu;",                           // 766
		    "boxUL;",                           // 767
		    "boxUR;",                           // 768
		    "boxUl;",                           // 769
		    "boxUr;",                           // 770
		    "boxV;",                            // 771
		    "boxVH;",                           // 772
		    "boxVL;",                           // 773
		    "boxVR;",                           // 774
		    "boxVh;",                           // 775
		    "boxVl;",                           // 776
		    "boxVr;",                           // 777
		    "boxbox;",                          // 778
		    "boxdL;",                           // 779
		    "boxdR;",                           // 780
		    "boxdl;",                           // 781
		    "boxdr;",                           // 782
		    "boxh;",                            // 783
		    "boxhD;",                           // 784
		    "boxhU;",                           // 785
		    "boxhd;",                           // 786
		    "boxhu;",                           // 787
		    "boxminus;",                        // 788
		    "boxplus;",                         // 789
		    "boxtimes;",                        // 790
		    "boxuL;",                           // 791
		    "boxuR;",                           // 792
		    "boxul;",                           // 793
		    "boxur;",                           // 794
		    "boxv;",                            // 795
		    "boxvH;",                           // 796
		    "boxvL;",                           // 797
		    "boxvR;",                           // 798
		    "boxvh;",                           // 799
		    "boxvl;",                           // 800
		    "boxvr;",                           // 801
		    "bprime;",                          // 802
		    "breve;",                           // 803
		    "brvbar",                           // 804
		    "brvbar;",                          // 805
		    "bscr;",                            // 806
		    "bsemi;",                           // 807
		    "bsim;",                            // 808
		    "bsime;",                           // 809
		    "bsol;",                            // 810
		    "bsolb;",                           // 811
		    "bsolhsub;",                        // 812
		    "bull;",                            // 813
		    "bullet;",                          // 814
		    "bump;",                            // 815
		    "bumpE;",                           // 816
		    "bumpe;",                           // 817
		    "bumpeq;",                          // 818
		    "cacute;",                          // 819
		    "cap;",                             // 820
		    "capand;",                          // 821
		    "capbrcup;",                        // 822
		    "capcap;",                          // 823
		    "capcup;",                          // 824
		    "capdot;",                          // 825
		    "caps;",                            // 826
		    "caret;",                           // 827
		    "caron;",                           // 828
		    "ccaps;",                           // 829
		    "ccaron;",                          // 830
		    "ccedil",                           // 831
		    "ccedil;",                          // 832
		    "ccirc;",                           // 833
		    "ccups;",                           // 834
		    "ccupssm;",                         // 835
		    "cdot;",                            // 836
		    "cedil",                            // 837
		    "cedil;",                           // 838
		    "cemptyv;",                         // 839
		    "cent",                             // 840
		    "cent;",                            // 841
		    "centerdot;",                       // 842
		    "cfr;",                             // 843
		    "chcy;",                            // 844
		    "check;",                           // 845
		    "checkmark;",                       // 846
		    "chi;",                             // 847
		    "cir;",                             // 848
		    "cirE;",                            // 849
		    "circ;",                            // 850
		    "circeq;",                          // 851
		    "circlearrowleft;",                 // 852
		    "circlearrowright;",                // 853
		    "circledR;",                        // 854
		    "circledS;",                        // 855
		    "circledast;",                      // 856
		    "circledcirc;",                     // 857
		    "circleddash;",                     // 858
		    "cire;",                            // 859
		    "cirfnint;",                        // 860
		    "cirmid;",                          // 861
		    "cirscir;",                         // 862
		    "clubs;",                           // 863
		    "clubsuit;",                        // 864
		    "colon;",                           // 865
		    "colone;",                          // 866
		    "coloneq;",                         // 867
		    "comma;",                           // 868
		    "commat;",                          // 869
		    "comp;",                            // 870
		    "compfn;",                          // 871
		    "complement;",                      // 872
		    "complexes;",                       // 873
		    "cong;",                            // 874
		    "congdot;",                         // 875
		    "conint;",                          // 876
		    "copf;",                            // 877
		    "coprod;",                          // 878
		    "copy",                             // 879
		    "copy;",                            // 880
		    "copysr;",                          // 881
		    "crarr;",                           // 882
		    "cross;",                           // 883
		    "cscr;",                            // 884
		    "csub;",                            // 885
		    "csube;",                           // 886
		    "csup;",                            // 887
		    "csupe;",                           // 888
		    "ctdot;",                           // 889
		    "cudarrl;",                         // 890
		    "cudarrr;",                         // 891
		    "cuepr;",                           // 892
		    "cuesc;",                           // 893
		    "cularr;",                          // 894
		    "cularrp;",                         // 895
		    "cup;",                             // 896
		    "cupbrcap;",                        // 897
		    "cupcap;",                          // 898
		    "cupcup;",                          // 899
		    "cupdot;",                          // 900
		    "cupor;",                           // 901
		    "cups;",                            // 902
		    "curarr;",                          // 903
		    "curarrm;",                         // 904
		    "curlyeqprec;",                     // 905
		    "curlyeqsucc;",                     // 906
		    "curlyvee;",                        // 907
		    "curlywedge;",                      // 908
		    "curren",                           // 909
		    "curren;",                          // 910
		    "curvearrowleft;",                  // 911
		    "curvearrowright;",                 // 912
		    "cuvee;",                           // 913
		    "cuwed;",                           // 914
		    "cwconint;",                        // 915
		    "cwint;",                           // 916
		    "cylcty;",                          // 917
		    "dArr;",                            // 918
		    "dHar;",                            // 919
		    "dagger;",                          // 920
		    "daleth;",                          // 921
		    "darr;",                            // 922
		    "dash;",                            // 923
		    "dashv;",                           // 924
		    "dbkarow;",                         // 925
		    "dblac;",                           // 926
		    "dcaron;",                          // 927
		    "dcy;",                             // 928
		    "dd;",                              // 929
		    "ddagger;",                         // 930
		    "ddarr;",                           // 931
		    "ddotseq;",                         // 932
		    "deg",                              // 933
		    "deg;",                             // 934
		    "delta;",                           // 935
		    "demptyv;",                         // 936
		    "dfisht;",                          // 937
		    "dfr;",                             // 938
		    "dharl;",                           // 939
		    "dharr;",                           // 940
		    "diam;",                            // 941
		    "diamond;",                         // 942
		    "diamondsuit;",                     // 943
		    "diams;",                           // 944
		    "die;",                             // 945
		    "digamma;",                         // 946
		    "disin;",                           // 947
		    "div;",                             // 948
		    "divide",                           // 949
		    "divide;",                          // 950
		    "divideontimes;",                   // 951
		    "divonx;",                          // 952
		    "djcy;",                            // 953
		    "dlcorn;",                          // 954
		    "dlcrop;",                          // 955
		    "dollar;",                          // 956
		    "dopf;",                            // 957
		    "dot;",                             // 958
		    "doteq;",                           // 959
		    "doteqdot;",                        // 960
		    "dotminus;",                        // 961
		    "dotplus;",                         // 962
		    "dotsquare;",                       // 963
		    "doublebarwedge;",                  // 964
		    "downarrow;",                       // 965
		    "downdownarrows;",                  // 966
		    "downharpoonleft;",                 // 967
		    "downharpoonright;",                // 968
		    "drbkarow;",                        // 969
		    "drcorn;",                          // 970
		    "drcrop;",                          // 971
		    "dscr;",                            // 972
		    "dscy;",                            // 973
		    "dsol;",                            // 974
		    "dstrok;",                          // 975
		    "dtdot;",                           // 976
		    "dtri;",                            // 977
		    "dtrif;",                           // 978
		    "duarr;",                           // 979
		    "duhar;",                           // 980
		    "dwangle;",                         // 981
		    "dzcy;",                            // 982
		    "dzigrarr;",                        // 983
		    "eDDot;",                           // 984
		    "eDot;",                            // 985
		    "eacute",                           // 986
		    "eacute;",                          // 987
		    "easter;",                          // 988
		    "ecaron;",                          // 989
		    "ecir;",                            // 990
		    "ecirc",                            // 991
		    "ecirc;",                           // 992
		    "ecolon;",                          // 993
		    "ecy;",                             // 994
		    "edot;",                            // 995
		    "ee;",                              // 996
		    "efDot;",                           // 997
		    "efr;",                             // 998
		    "eg;",                              // 999
		    "egrave",                           // 1000
		    "egrave;",                          // 1001
		    "egs;",                             // 1002
		    "egsdot;",                          // 1003
		    "el;",                              // 1004
		    "elinters;",                        // 1005
		    "ell;",                             // 1006
		    "els;",                             // 1007
		    "elsdot;",                          // 1008
		    "emacr;",                           // 1009
		    "empty;",                           // 1010
		    "emptyset;",                        // 1011
		    "emptyv;",                          // 1012
		    "emsp13;",                          // 1013
		    "emsp14;",                          // 1014
		    "emsp;",                            // 1015
		    "eng;",                             // 1016
		    "ensp;",                            // 1017
		    "eogon;",                           // 1018
		    "eopf;",                            // 1019
		    "epar;",                            // 1020
		    "eparsl;",                          // 1021
		    "eplus;",                           // 1022
		    "epsi;",                            // 1023
		    "epsilon;",                         // 1024
		    "epsiv;",                           // 1025
		    "eqcirc;",                          // 1026
		    "eqcolon;",                         // 1027
		    "eqsim;",                           // 1028
		    "eqslantgtr;",                      // 1029
		    "eqslantless;",                     // 1030
		    "equals;",                          // 1031
		    "equest;",                          // 1032
		    "equiv;",                           // 1033
		    "equivDD;",                         // 1034
		    "eqvparsl;",                        // 1035
		    "erDot;",                           // 1036
		    "erarr;",                           // 1037
		    "escr;",                            // 1038
		    "esdot;",                           // 1039
		    "esim;",                            // 1040
		    "eta;",                             // 1041
		    "eth",                              // 1042
		    "eth;",                             // 1043
		    "euml",                             // 1044
		    "euml;",                            // 1045
		    "euro;",                            // 1046
		    "excl;",                            // 1047
		    "exist;",                           // 1048
		    "expectation;",                     // 1049
		    "exponentiale;",                    // 1050
		    "fallingdotseq;",                   // 1051
		    "fcy;",                             // 1052
		    "female;",                          // 1053
		    "ffilig;",                          // 1054
		    "fflig;",                           // 1055
		    "ffllig;",                          // 1056
		    "ffr;",                             // 1057
		    "filig;",                           // 1058
		    "fjlig;",                           // 1059
		    "flat;",                            // 1060
		    "fllig;",                           // 1061
		    "fltns;",                           // 1062
		    "fnof;",                            // 1063
		    "fopf;",                            // 1064
		    "forall;",                          // 1065
		    "fork;",                            // 1066
		    "forkv;",                           // 1067
		    "fpartint;",                        // 1068
		    "frac12",                           // 1069
		    "frac12;",                          // 1070
		    "frac13;",                          // 1071
		    "frac14",                           // 1072
		    "frac14;",                          // 1073
		    "frac15;",                          // 1074
		    "frac16;",                          // 1075
		    "frac18;",                          // 1076
		    "frac23;",                          // 1077
		    "frac25;",                          // 1078
		    "frac34",                           // 1079
		    "frac34;",                          // 1080
		    "frac35;",                          // 1081
		    "frac38;",                          // 1082
		    "frac45;",                          // 1083
		    "frac56;",                          // 1084
		    "frac58;",                          // 1085
		    "frac78;",                          // 1086
		    "frasl;",                           // 1087
		    "frown;",                           // 1088
		    "fscr;",                            // 1089
		    "gE;",                              // 1090
		    "gEl;",                             // 1091
		    "gacute;",                          // 1092
		    "gamma;",                           // 1093
		    "gammad;",                          // 1094
		    "gap;",                             // 1095
		    "gbreve;",                          // 1096
		    "gcirc;",                           // 1097
		    "gcy;",                             // 1098
		    "gdot;",                            // 1099
		    "ge;",                              // 1100
		    "gel;",                             // 1101
		    "geq;",                             // 1102
		    "geqq;",                            // 1103
		    "geqslant;",                        // 1104
		    "ges;",                             // 1105
		    "gescc;",                           // 1106
		    "gesdot;",                          // 1107
		    "gesdoto;",                         // 1108
		    "gesdotol;",                        // 1109
		    "gesl;",                            // 1110
		    "gesles;",                          // 1111
		    "gfr;",                             // 1112
		    "gg;",                              // 1113
		    "ggg;",                             // 1114
		    "gimel;",                           // 1115
		    "gjcy;",                            // 1116
		    "gl;",                              // 1117
		    "glE;",                             // 1118
		    "gla;",                             // 1119
		    "glj;",                             // 1120
		    "gnE;",                             // 1121
		    "gnap;",                            // 1122
		    "gnapprox;",                        // 1123
		    "gne;",                             // 1124
		    "gneq;",                            // 1125
		    "gneqq;",                           // 1126
		    "gnsim;",                           // 1127
		    "gopf;",                            // 1128
		    "grave;",                           // 1129
		    "gscr;",                            // 1130
		    "gsim;",                            // 1131
		    "gsime;",                           // 1132
		    "gsiml;",                           // 1133
		    "gt",                               // 1134
		    "gt;",                              // 1135
		    "gtcc;",                            // 1136
		    "gtcir;",                           // 1137
		    "gtdot;",                           // 1138
		    "gtlPar;",                          // 1139
		    "gtquest;",                         // 1140
		    "gtrapprox;",                       // 1141
		    "gtrarr;",                          // 1142
		    "gtrdot;",                          // 1143
		    "gtreqless;",                       // 1144
		    "gtreqqless;",                      // 1145
		    "gtrless;",                         // 1146
		    "gtrsim;",                          // 1147
		    "gvertneqq;",                       // 1148
		    "gvnE;",                            // 1149
		    "hArr;",                            // 1150
		    "hairsp;",                          // 1151
		    "half;",                            // 1152
		    "hamilt;",                          // 1153
		    "hardcy;",                          // 1154
		    "harr;",                            // 1155
		    "harrcir;",                         // 1156
		    "harrw;",                           // 1157
		    "hbar;",                            // 1158
		    "hcirc;",                           // 1159
		    "hearts;",                          // 1160
		    "heartsuit;",                       // 1161
		    "hellip;",                          // 1162
		    "hercon;",                          // 1163
		    "hfr;",                             // 1164
		    "hksearow;",                        // 1165
		    "hkswarow;",                        // 1166
		    "hoarr;",                           // 1167
		    "homtht;",                          // 1168
		    "hookleftarrow;",                   // 1169
		    "hookrightarrow;",                  // 1170
		    "hopf;",                            // 1171
		    "horbar;",                          // 1172
		    "hscr;",                            // 1173
		    "hslash;",                          // 1174
		    "hstrok;",                          // 1175
		    "hybull;",                          // 1176
		    "hyphen;",                          // 1177
		    "iacute",                           // 1178
		    "iacute;",                          // 1179
		    "ic;",                              // 1180
		    "icirc",                            // 1181
		    "icirc;",                           // 1182
		    "icy;",                             // 1183
		    "iecy;",                            // 1184
		    "iexcl",                            // 1185
		    "iexcl;",                           // 1186
		    "iff;",                             // 1187
		    "ifr;",                             // 1188
		    "igrave",                           // 1189
		    "igrave;",                          // 1190
		    "ii;",                              // 1191
		    "iiiint;",                          // 1192
		    "iiint;",                           // 1193
		    "iinfin;",                          // 1194
		    "iiota;",                           // 1195
		    "ijlig;",                           // 1196
		    "imacr;",                           // 1197
		    "image;",                           // 1198
		    "imagline;",                        // 1199
		    "imagpart;",                        // 1200
		    "imath;",                           // 1201
		    "imof;",                            // 1202
		    "imped;",                           // 1203
		    "in;",                              // 1204
		    "incare;",                          // 1205
		    "infin;",                           // 1206
		    "infintie;",                        // 1207
		    "inodot;",                          // 1208
		    "int;",                             // 1209
		    "intcal;",                          // 1210
		    "integers;",                        // 1211
		    "intercal;",                        // 1212
		    "intlarhk;",                        // 1213
		    "intprod;",                         // 1214
		    "iocy;",                            // 1215
		    "iogon;",                           // 1216
		    "iopf;",                            // 1217
		    "iota;",                            // 1218
		    "iprod;",                           // 1219
		    "iquest",                           // 1220
		    "iquest;",                          // 1221
		    "iscr;",                            // 1222
		    "isin;",                            // 1223
		    "isinE;",                           // 1224
		    "isindot;",                         // 1225
		    "isins;",                           // 1226
		    "isinsv;",                          // 1227
		    "isinv;",                           // 1228
		    "it;",                              // 1229
		    "itilde;",                          // 1230
		    "iukcy;",                           // 1231
		    "iuml",                             // 1232
		    "iuml;",                            // 1233
		    "jcirc;",                           // 1234
		    "jcy;",                             // 1235
		    "jfr;",                             // 1236
		    "jmath;",                           // 1237
		    "jopf;",                            // 1238
		    "jscr;",                            // 1239
		    "jsercy;",                          // 1240
		    "jukcy;",                           // 1241
		    "kappa;",                           // 1242
		    "kappav;",                          // 1243
		    "kcedil;",                          // 1244
		    "kcy;",                             // 1245
		    "kfr;",                             // 1246
		    "kgreen;",                          // 1247
		    "khcy;",                            // 1248
		    "kjcy;",                            // 1249
		    "kopf;",                            // 1250
		    "kscr;",                            // 1251
		    "lAarr;",                           // 1252
		    "lArr;",                            // 1253
		    "lAtail;",                          // 1254
		    "lBarr;",                           // 1255
		    "lE;",                              // 1256
		    "lEg;",                             // 1257
		    "lHar;",                            // 1258
		    "lacute;",                          // 1259
		    "laemptyv;",                        // 1260
		    "lagran;",                          // 1261
		    "lambda;",                          // 1262
		    "lang;",                            // 1263
		    "langd;",                           // 1264
		    "langle;",                          // 1265
		    "lap;",                             // 1266
		    "laquo",                            // 1267
		    "laquo;",                           // 1268
		    "larr;",                            // 1269
		    "larrb;",                           // 1270
		    "larrbfs;",                         // 1271
		    "larrfs;",                          // 1272
		    "larrhk;",                          // 1273
		    "larrlp;",                          // 1274
		    "larrpl;",                          // 1275
		    "larrsim;",                         // 1276
		    "larrtl;",                          // 1277
		    "lat;",                             // 1278
		    "latail;",                          // 1279
		    "late;",                            // 1280
		    "lates;",                           // 1281
		    "lbarr;",                           // 1282
		    "lbbrk;",                           // 1283
		    "lbrace;",                          // 1284
		    "lbrack;",                          // 1285
		    "lbrke;",                           // 1286
		    "lbrksld;",                         // 1287
		    "lbrkslu;",                         // 1288
		    "lcaron;",                          // 1289
		    "lcedil;",                          // 1290
		    "lceil;",                           // 1291
		    "lcub;",                            // 1292
		    "lcy;",                             // 1293
		    "ldca;",                            // 1294
		    "ldquo;",                           // 1295
		    "ldquor;",                          // 1296
		    "ldrdhar;",                         // 1297
		    "ldrushar;",                        // 1298
		    "ldsh;",                            // 1299
		    "le;",                              // 1300
		    "leftarrow;",                       // 1301
		    "leftarrowtail;",                   // 1302
		    "leftharpoondown;",                 // 1303
		    "leftharpoonup;",                   // 1304
		    "leftleftarrows;",                  // 1305
		    "leftrightarrow;",                  // 1306
		    "leftrightarrows;",                 // 1307
		    "leftrightharpoons;",               // 1308
		    "leftrightsquigarrow;",             // 1309
		    "leftthreetimes;",                  // 1310
		    "leg;",                             // 1311
		    "leq;",                             // 1312
		    "leqq;",                            // 1313
		    "leqslant;",                        // 1314
		    "les;",                             // 1315
		    "lescc;",                           // 1316
		    "lesdot;",                          // 1317
		    "lesdoto;",                         // 1318
		    "lesdotor;",                        // 1319
		    "lesg;",                            // 1320
		    "lesges;",                          // 1321
		    "lessapprox;",                      // 1322
		    "lessdot;",                         // 1323
		    "lesseqgtr;",                       // 1324
		    "lesseqqgtr;",                      // 1325
		    "lessgtr;",                         // 1326
		    "lesssim;",                         // 1327
		    "lfisht;",                          // 1328
		    "lfloor;",                          // 1329
		    "lfr;",                             // 1330
		    "lg;",                              // 1331
		    "lgE;",                             // 1332
		    "lhard;",                           // 1333
		    "lharu;",                           // 1334
		    "lharul;",                          // 1335
		    "lhblk;",                           // 1336
		    "ljcy;",                            // 1337
		    "ll;",                              // 1338
		    "llarr;",                           // 1339
		    "llcorner;",                        // 1340
		    "llhard;",                          // 1341
		    "lltri;",                           // 1342
		    "lmidot;",                          // 1343
		    "lmoust;",                          // 1344
		    "lmoustache;",                      // 1345
		    "lnE;",                             // 1346
		    "lnap;",                            // 1347
		    "lnapprox;",                        // 1348
		    "lne;",                             // 1349
		    "lneq;",                            // 1350
		    "lneqq;",                           // 1351
		    "lnsim;",                           // 1352
		    "loang;",                           // 1353
		    "loarr;",                           // 1354
		    "lobrk;",                           // 1355
		    "longleftarrow;",                   // 1356
		    "longleftrightarrow;",              // 1357
		    "longmapsto;",                      // 1358
		    "longrightarrow;",                  // 1359
		    "looparrowleft;",                   // 1360
		    "looparrowright;",                  // 1361
		    "lopar;",                           // 1362
		    "lopf;",                            // 1363
		    "loplus;",                          // 1364
		    "lotimes;",                         // 1365
		    "lowast;",                          // 1366
		    "lowbar;",                          // 1367
		    "loz;",                             // 1368
		    "lozenge;",                         // 1369
		    "lozf;",                            // 1370
		    "lpar;",                            // 1371
		    "lparlt;",                          // 1372
		    "lrarr;",                           // 1373
		    "lrcorner;",                        // 1374
		    "lrhar;",                           // 1375
		    "lrhard;",                          // 1376
		    "lrm;",                             // 1377
		    "lrtri;",                           // 1378
		    "lsaquo;",                          // 1379
		    "lscr;",                            // 1380
		    "lsh;",                             // 1381
		    "lsim;",                            // 1382
		    "lsime;",                           // 1383
		    "lsimg;",                           // 1384
		    "lsqb;",                            // 1385
		    "lsquo;",                           // 1386
		    "lsquor;",                          // 1387
		    "lstrok;",                          // 1388
		    "lt",                               // 1389
		    "lt;",                              // 1390
		    "ltcc;",                            // 1391
		    "ltcir;",                           // 1392
		    "ltdot;",                           // 1393
		    "lthree;",                          // 1394
		    "ltimes;",                          // 1395
		    "ltlarr;",                          // 1396
		    "ltquest;",                         // 1397
		    "ltrPar;",                          // 1398
		    "ltri;",                            // 1399
		    "ltrie;",                           // 1400
		    "ltrif;",                           // 1401
		    "lurdshar;",                        // 1402
		    "luruhar;",                         // 1403
		    "lvertneqq;",                       // 1404
		    "lvnE;",                            // 1405
		    "mDDot;",                           // 1406
		    "macr",                             // 1407
		    "macr;",                            // 1408
		    "male;",                            // 1409
		    "malt;",                            // 1410
		    "maltese;",                         // 1411
		    "map;",                             // 1412
		    "mapsto;",                          // 1413
		    "mapstodown;",                      // 1414
		    "mapstoleft;",                      // 1415
		    "mapstoup;",                        // 1416
		    "marker;",                          // 1417
		    "mcomma;",                          // 1418
		    "mcy;",                             // 1419
		    "mdash;",                           // 1420
		    "measuredangle;",                   // 1421
		    "mfr;",                             // 1422
		    "mho;",                             // 1423
		    "micro",                            // 1424
		    "micro;",                           // 1425
		    "mid;",                             // 1426
		    "midast;",                          // 1427
		    "midcir;",                          // 1428
		    "middot",                           // 1429
		    "middot;",                          // 1430
		    "minus;",                           // 1431
		    "minusb;",                          // 1432
		    "minusd;",                          // 1433
		    "minusdu;",                         // 1434
		    "mlcp;",                            // 1435
		    "mldr;",                            // 1436
		    "mnplus;",                          // 1437
		    "models;",                          // 1438
		    "mopf;",                            // 1439
		    "mp;",                              // 1440
		    "mscr;",                            // 1441
		    "mstpos;",                          // 1442
		    "mu;",                              // 1443
		    "multimap;",                        // 1444
		    "mumap;",                           // 1445
		    "nGg;",                             // 1446
		    "nGt;",                             // 1447
		    "nGtv;",                            // 1448
		    "nLeftarrow;",                      // 1449
		    "nLeftrightarrow;",                 // 1450
		    "nLl;",                             // 1451
		    "nLt;",                             // 1452
		    "nLtv;",                            // 1453
		    "nRightarrow;",                     // 1454
		    "nVDash;",                          // 1455
		    "nVdash;",                          // 1456
		    "nabla;",                           // 1457
		    "nacute;",                          // 1458
		    "nang;",                            // 1459
		    "nap;",                             // 1460
		    "napE;",                            // 1461
		    "napid;",                           // 1462
		    "napos;",                           // 1463
		    "napprox;",                         // 1464
		    "natur;",                           // 1465
		    "natural;",                         // 1466
		    "naturals;",                        // 1467
		    "nbsp",                             // 1468
		    "nbsp;",                            // 1469
		    "nbump;",                           // 1470
		    "nbumpe;",                          // 1471
		    "ncap;",                            // 1472
		    "ncaron;",                          // 1473
		    "ncedil;",                          // 1474
		    "ncong;",                           // 1475
		    "ncongdot;",                        // 1476
		    "ncup;",                            // 1477
		    "ncy;",                             // 1478
		    "ndash;",                           // 1479
		    "ne;",                              // 1480
		    "neArr;",                           // 1481
		    "nearhk;",                          // 1482
		    "nearr;",                           // 1483
		    "nearrow;",                         // 1484
		    "nedot;",                           // 1485
		    "nequiv;",                          // 1486
		    "nesear;",                          // 1487
		    "nesim;",                           // 1488
		    "nexist;",                          // 1489
		    "nexists;",                         // 1490
		    "nfr;",                             // 1491
		    "ngE;",                             // 1492
		    "nge;",                             // 1493
		    "ngeq;",                            // 1494
		    "ngeqq;",                           // 1495
		    "ngeqslant;",                       // 1496
		    "nges;",                            // 1497
		    "ngsim;",                           // 1498
		    "ngt;",                             // 1499
		    "ngtr;",                            // 1500
		    "nhArr;",                           // 1501
		    "nharr;",                           // 1502
		    "nhpar;",                           // 1503
		    "ni;",                              // 1504
		    "nis;",                             // 1505
		    "nisd;",                            // 1506
		    "niv;",                             // 1507
		    "njcy;",                            // 1508
		    "nlArr;",                           // 1509
		    "nlE;",                             // 1510
		    "nlarr;",                           // 1511
		    "nldr;",                            // 1512
		    "nle;",                             // 1513
		    "nleftarrow;",                      // 1514
		    "nleftrightarrow;",                 // 1515
		    "nleq;",                            // 1516
		    "nleqq;",                           // 1517
		    "nleqslant;",                       // 1518
		    "nles;",                            // 1519
		    "nless;",                           // 1520
		    "nlsim;",                           // 1521
		    "nlt;",                             // 1522
		    "nltri;",                           // 1523
		    "nltrie;",                          // 1524
		    "nmid;",                            // 1525
		    "nopf;",                            // 1526
		    "not",                              // 1527
		    "not;",                             // 1528
		    "notin;",                           // 1529
		    "notinE;",                          // 1530
		    "notindot;",                        // 1531
		    "notinva;",                         // 1532
		    "notinvb;",                         // 1533
		    "notinvc;",                         // 1534
		    "notni;",                           // 1535
		    "notniva;",                         // 1536
		    "notnivb;",                         // 1537
		    "notnivc;",                         // 1538
		    "npar;",                            // 1539
		    "nparallel;",                       // 1540
		    "nparsl;",                          // 1541
		    "npart;",                           // 1542
		    "npolint;",                         // 1543
		    "npr;",                             // 1544
		    "nprcue;",                          // 1545
		    "npre;",                            // 1546
		    "nprec;",                           // 1547
		    "npreceq;",                         // 1548
		    "nrArr;",                           // 1549
		    "nrarr;",                           // 1550
		    "nrarrc;",                          // 1551
		    "nrarrw;",                          // 1552
		    "nrightarrow;",                     // 1553
		    "nrtri;",                           // 1554
		    "nrtrie;",                          // 1555
		    "nsc;",                             // 1556
		    "nsccue;",                          // 1557
		    "nsce;",                            // 1558
		    "nscr;",                            // 1559
		    "nshortmid;",                       // 1560
		    "nshortparallel;",                  // 1561
		    "nsim;",                            // 1562
		    "nsime;",                           // 1563
		    "nsimeq;",                          // 1564
		    "nsmid;",                           // 1565
		    "nspar;",                           // 1566
		    "nsqsube;",                         // 1567
		    "nsqsupe;",                         // 1568
		    "nsub;",                            // 1569
		    "nsubE;",                           // 1570
		    "nsube;",                           // 1571
		    "nsubset;",                         // 1572
		    "nsubseteq;",                       // 1573
		    "nsubseteqq;",                      // 1574
		    "nsucc;",                           // 1575
		    "nsucceq;",                         // 1576
		    "nsup;",                            // 1577
		    "nsupE;",                           // 1578
		    "nsupe;",                           // 1579
		    "nsupset;",                         // 1580
		    "nsupseteq;",                       // 1581
		    "nsupseteqq;",                      // 1582
		    "ntgl;",                            // 1583
		    "ntilde",                           // 1584
		    "ntilde;",                          // 1585
		    "ntlg;",                            // 1586
		    "ntriangleleft;",                   // 1587
		    "ntrianglelefteq;",                 // 1588
		    "ntriangleright;",                  // 1589
		    "ntrianglerighteq;",                // 1590
		    "nu;",                              // 1591
		    "num;",                             // 1592
		    "numero;",                          // 1593
		    "numsp;",                           // 1594
		    "nvDash;",                          // 1595
		    "nvHarr;",                          // 1596
		    "nvap;",                            // 1597
		    "nvdash;",                          // 1598
		    "nvge;",                            // 1599
		    "nvgt;",                            // 1600
		    "nvinfin;",                         // 1601
		    "nvlArr;",                          // 1602
		    "nvle;",                            // 1603
		    "nvlt;",                            // 1604
		    "nvltrie;",                         // 1605
		    "nvrArr;",                          // 1606
		    "nvrtrie;",                         // 1607
		    "nvsim;",                           // 1608
		    "nwArr;",                           // 1609
		    "nwarhk;",                          // 1610
		    "nwarr;",                           // 1611
		    "nwarrow;",                         // 1612
		    "nwnear;",                          // 1613
		    "oS;",                              // 1614
		    "oacute",                           // 1615
		    "oacute;",                          // 1616
		    "oast;",                            // 1617
		    "ocir;",                            // 1618
		    "ocirc",                            // 1619
		    "ocirc;",                           // 1620
		    "ocy;",                             // 1621
		    "odash;",                           // 1622
		    "odblac;",                          // 1623
		    "odiv;",                            // 1624
		    "odot;",                            // 1625
		    "odsold;",                          // 1626
		    "oelig;",                           // 1627
		    "ofcir;",                           // 1628
		    "ofr;",                             // 1629
		    "ogon;",                            // 1630
		    "ograve",                           // 1631
		    "ograve;",                          // 1632
		    "ogt;",                             // 1633
		    "ohbar;",                           // 1634
		    "ohm;",                             // 1635
		    "oint;",                            // 1636
		    "olarr;",                           // 1637
		    "olcir;",                           // 1638
		    "olcross;",                         // 1639
		    "oline;",                           // 1640
		    "olt;",                             // 1641
		    "omacr;",                           // 1642
		    "omega;",                           // 1643
		    "omicron;",                         // 1644
		    "omid;",                            // 1645
		    "ominus;",                          // 1646
		    "oopf;",                            // 1647
		    "opar;",                            // 1648
		    "operp;",                           // 1649
		    "oplus;",                           // 1650
		    "or;",                              // 1651
		    "orarr;",                           // 1652
		    "ord;",                             // 1653
		    "order;",                           // 1654
		    "orderof;",                         // 1655
		    "ordf",                             // 1656
		    "ordf;",                            // 1657
		    "ordm",                             // 1658
		    "ordm;",                            // 1659
		    "origof;",                          // 1660
		    "oror;",                            // 1661
		    "orslope;",                         // 1662
		    "orv;",                             // 1663
		    "oscr;",                            // 1664
		    "oslash",                           // 1665
		    "oslash;",                          // 1666
		    "osol;",                            // 1667
		    "otilde",                           // 1668
		    "otilde;",                          // 1669
		    "otimes;",                          // 1670
		    "otimesas;",                        // 1671
		    "ouml",                             // 1672
		    "ouml;",                            // 1673
		    "ovbar;",                           // 1674
		    "par;",                             // 1675
		    "para",                             // 1676
		    "para;",                            // 1677
		    "parallel;",                        // 1678
		    "parsim;",                          // 1679
		    "parsl;",                           // 1680
		    "part;",                            // 1681
		    "pcy;",                             // 1682
		    "percnt;",                          // 1683
		    "period;",                          // 1684
		    "permil;",                          // 1685
		    "perp;",                            // 1686
		    "pertenk;",                         // 1687
		    "pfr;",                             // 1688
		    "phi;",                             // 1689
		    "phiv;",                            // 1690
		    "phmmat;",                          // 1691
		    "phone;",                           // 1692
		    "pi;",                              // 1693
		    "pitchfork;",                       // 1694
		    "piv;",                             // 1695
		    "planck;",                          // 1696
		    "planckh;",                         // 1697
		    "plankv;",                          // 1698
		    "plus;",                            // 1699
		    "plusacir;",                        // 1700
		    "plusb;",                           // 1701
		    "pluscir;",                         // 1702
		    "plusdo;",                          // 1703
		    "plusdu;",                          // 1704
		    "pluse;",                           // 1705
		    "plusmn",                           // 1706
		    "plusmn;",                          // 1707
		    "plussim;",                         // 1708
		    "plustwo;",                         // 1709
		    "pm;",                              // 1710
		    "pointint;",                        // 1711
		    "popf;",                            // 1712
		    "pound",                            // 1713
		    "pound;",                           // 1714
		    "pr;",                              // 1715
		    "prE;",                             // 1716
		    "prap;",                            // 1717
		    "prcue;",                           // 1718
		    "pre;",                             // 1719
		    "prec;",                            // 1720
		    "precapprox;",                      // 1721
		    "preccurlyeq;",                     // 1722
		    "preceq;",                          // 1723
		    "precnapprox;",                     // 1724
		    "precneqq;",                        // 1725
		    "precnsim;",                        // 1726
		    "precsim;",                         // 1727
		    "prime;",                           // 1728
		    "primes;",                          // 1729
		    "prnE;",                            // 1730
		    "prnap;",                           // 1731
		    "prnsim;",                          // 1732
		    "prod;",                            // 1733
		    "profalar;",                        // 1734
		    "profline;",                        // 1735
		    "profsurf;",                        // 1736
		    "prop;",                            // 1737
		    "propto;",                          // 1738
		    "prsim;",                           // 1739
		    "prurel;",                          // 1740
		    "pscr;",                            // 1741
		    "psi;",                             // 1742
		    "puncsp;",                          // 1743
		    "qfr;",                             // 1744
		    "qint;",                            // 1745
		    "qopf;",                            // 1746
		    "qprime;",                          // 1747
		    "qscr;",                            // 1748
		    "quaternions;",                     // 1749
		    "quatint;",                         // 1750
		    "quest;",                           // 1751
		    "questeq;",                         // 1752
		    "quot",                             // 1753
		    "quot;",                            // 1754
		    "rAarr;",                           // 1755
		    "rArr;",                            // 1756
		    "rAtail;",                          // 1757
		    "rBarr;",                           // 1758
		    "rHar;",                            // 1759
		    "race;",                            // 1760
		    "racute;",                          // 1761
		    "radic;",                           // 1762
		    "raemptyv;",                        // 1763
		    "rang;",                            // 1764
		    "rangd;",                           // 1765
		    "range;",                           // 1766
		    "rangle;",                          // 1767
		    "raquo",                            // 1768
		    "raquo;",                           // 1769
		    "rarr;",                            // 1770
		    "rarrap;",                          // 1771
		    "rarrb;",                           // 1772
		    "rarrbfs;",                         // 1773
		    "rarrc;",                           // 1774
		    "rarrfs;",                          // 1775
		    "rarrhk;",                          // 1776
		    "rarrlp;",                          // 1777
		    "rarrpl;",                          // 1778
		    "rarrsim;",                         // 1779
		    "rarrtl;",                          // 1780
		    "rarrw;",                           // 1781
		    "ratail;",                          // 1782
		    "ratio;",                           // 1783
		    "rationals;",                       // 1784
		    "rbarr;",                           // 1785
		    "rbbrk;",                           // 1786
		    "rbrace;",                          // 1787
		    "rbrack;",                          // 1788
		    "rbrke;",                           // 1789
		    "rbrksld;",                         // 1790
		    "rbrkslu;",                         // 1791
		    "rcaron;",                          // 1792
		    "rcedil;",                          // 1793
		    "rceil;",                           // 1794
		    "rcub;",                            // 1795
		    "rcy;",                             // 1796
		    "rdca;",                            // 1797
		    "rdldhar;",                         // 1798
		    "rdquo;",                           // 1799
		    "rdquor;",                          // 1800
		    "rdsh;",                            // 1801
		    "real;",                            // 1802
		    "realine;",                         // 1803
		    "realpart;",                        // 1804
		    "reals;",                           // 1805
		    "rect;",                            // 1806
		    "reg",                              // 1807
		    "reg;",                             // 1808
		    "rfisht;",                          // 1809
		    "rfloor;",                          // 1810
		    "rfr;",                             // 1811
		    "rhard;",                           // 1812
		    "rharu;",                           // 1813
		    "rharul;",                          // 1814
		    "rho;",                             // 1815
		    "rhov;",                            // 1816
		    "rightarrow;",                      // 1817
		    "rightarrowtail;",                  // 1818
		    "rightharpoondown;",                // 1819
		    "rightharpoonup;",                  // 1820
		    "rightleftarrows;",                 // 1821
		    "rightleftharpoons;",               // 1822
		    "rightrightarrows;",                // 1823
		    "rightsquigarrow;",                 // 1824
		    "rightthreetimes;",                 // 1825
		    "ring;",                            // 1826
		    "risingdotseq;",                    // 1827
		    "rlarr;",                           // 1828
		    "rlhar;",                           // 1829
		    "rlm;",                             // 1830
		    "rmoust;",                          // 1831
		    "rmoustache;",                      // 1832
		    "rnmid;",                           // 1833
		    "roang;",                           // 1834
		    "roarr;",                           // 1835
		    "robrk;",                           // 1836
		    "ropar;",                           // 1837
		    "ropf;",                            // 1838
		    "roplus;",                          // 1839
		    "rotimes;",                         // 1840
		    "rpar;",                            // 1841
		    "rpargt;",                          // 1842
		    "rppolint;",                        // 1843
		    "rrarr;",                           // 1844
		    "rsaquo;",                          // 1845
		    "rscr;",                            // 1846
		    "rsh;",                             // 1847
		    "rsqb;",                            // 1848
		    "rsquo;",                           // 1849
		    "rsquor;",                          // 1850
		    "rthree;",                          // 1851
		    "rtimes;",                          // 1852
		    "rtri;",                            // 1853
		    "rtrie;",                           // 1854
		    "rtrif;",                           // 1855
		    "rtriltri;",                        // 1856
		    "ruluhar;",                         // 1857
		    "rx;",                              // 1858
		    "sacute;",                          // 1859
		    "sbquo;",                           // 1860
		    "sc;",                              // 1861
		    "scE;",                             // 1862
		    "scap;",                            // 1863
		    "scaron;",                          // 1864
		    "sccue;",                           // 1865
		    "sce;",                             // 1866
		    "scedil;",                          // 1867
		    "scirc;",                           // 1868
		    "scnE;",                            // 1869
		    "scnap;",                           // 1870
		    "scnsim;",                          // 1871
		    "scpolint;",                        // 1872
		    "scsim;",                           // 1873
		    "scy;",                             // 1874
		    "sdot;",                            // 1875
		    "sdotb;",                           // 1876
		    "sdote;",                           // 1877
		    "seArr;",                           // 1878
		    "searhk;",                          // 1879
		    "searr;",                           // 1880
		    "searrow;",                         // 1881
		    "sect",                             // 1882
		    "sect;",                            // 1883
		    "semi;",                            // 1884
		    "seswar;",                          // 1885
		    "setminus;",                        // 1886
		    "setmn;",                           // 1887
		    "sext;",                            // 1888
		    "sfr;",                             // 1889
		    "sfrown;",                          // 1890
		    "sharp;",                           // 1891
		    "shchcy;",                          // 1892
		    "shcy;",                            // 1893
		    "shortmid;",                        // 1894
		    "shortparallel;",                   // 1895
		    "shy",                              // 1896
		    "shy;",                             // 1897
		    "sigma;",                           // 1898
		    "sigmaf;",                          // 1899
		    "sigmav;",                          // 1900
		    "sim;",                             // 1901
		    "simdot;",                          // 1902
		    "sime;",                            // 1903
		    "simeq;",                           // 1904
		    "simg;",                            // 1905
		    "simgE;",                           // 1906
		    "siml;",                            // 1907
		    "simlE;",                           // 1908
		    "simne;",                           // 1909
		    "simplus;",                         // 1910
		    "simrarr;",                         // 1911
		    "slarr;",                           // 1912
		    "smallsetminus;",                   // 1913
		    "smashp;",                          // 1914
		    "smeparsl;",                        // 1915
		    "smid;",                            // 1916
		    "smile;",                           // 1917
		    "smt;",                             // 1918
		    "smte;",                            // 1919
		    "smtes;",                           // 1920
		    "softcy;",                          // 1921
		    "sol;",                             // 1922
		    "solb;",                            // 1923
		    "solbar;",                          // 1924
		    "sopf;",                            // 1925
		    "spades;",                          // 1926
		    "spadesuit;",                       // 1927
		    "spar;",                            // 1928
		    "sqcap;",                           // 1929
		    "sqcaps;",                          // 1930
		    "sqcup;",                           // 1931
		    "sqcups;",                          // 1932
		    "sqsub;",                           // 1933
		    "sqsube;",                          // 1934
		    "sqsubset;",                        // 1935
		    "sqsubseteq;",                      // 1936
		    "sqsup;",                           // 1937
		    "sqsupe;",                          // 1938
		    "sqsupset;",                        // 1939
		    "sqsupseteq;",                      // 1940
		    "squ;",                             // 1941
		    "square;",                          // 1942
		    "squarf;",                          // 1943
		    "squf;",                            // 1944
		    "srarr;",                           // 1945
		    "sscr;",                            // 1946
		    "ssetmn;",                          // 1947
		    "ssmile;",                          // 1948
		    "sstarf;",                          // 1949
		    "star;",                            // 1950
		    "starf;",                           // 1951
		    "straightepsilon;",                 // 1952
		    "straightphi;",                     // 1953
		    "strns;",                           // 1954
		    "sub;",                             // 1955
		    "subE;",                            // 1956
		    "subdot;",                          // 1957
		    "sube;",                            // 1958
		    "subedot;",                         // 1959
		    "submult;",                         // 1960
		    "subnE;",                           // 1961
		    "subne;",                           // 1962
		    "subplus;",                         // 1963
		    "subrarr;",                         // 1964
		    "subset;",                          // 1965
		    "subseteq;",                        // 1966
		    "subseteqq;",                       // 1967
		    "subsetneq;",                       // 1968
		    "subsetneqq;",                      // 1969
		    "subsim;",                          // 1970
		    "subsub;",                          // 1971
		    "subsup;",                          // 1972
		    "succ;",                            // 1973
		    "succapprox;",                      // 1974
		    "succcurlyeq;",                     // 1975
		    "succeq;",                          // 1976
		    "succnapprox;",                     // 1977
		    "succneqq;",                        // 1978
		    "succnsim;",                        // 1979
		    "succsim;",                         // 1980
		    "sum;",                             // 1981
		    "sung;",                            // 1982
		    "sup1",                             // 1983
		    "sup1;",                            // 1984
		    "sup2",                             // 1985
		    "sup2;",                            // 1986
		    "sup3",                             // 1987
		    "sup3;",                            // 1988
		    "sup;",                             // 1989
		    "supE;",                            // 1990
		    "supdot;",                          // 1991
		    "supdsub;",                         // 1992
		    "supe;",                            // 1993
		    "supedot;",                         // 1994
		    "suphsol;",                         // 1995
		    "suphsub;",                         // 1996
		    "suplarr;",                         // 1997
		    "supmult;",                         // 1998
		    "supnE;",                           // 1999
		    "supne;",                           // 2000
		    "supplus;",                         // 2001
		    "supset;",                          // 2002
		    "supseteq;",                        // 2003
		    "supseteqq;",                       // 2004
		    "supsetneq;",                       // 2005
		    "supsetneqq;",                      // 2006
		    "supsim;",                          // 2007
		    "supsub;",                          // 2008
		    "supsup;",                          // 2009
		    "swArr;",                           // 2010
		    "swarhk;",                          // 2011
		    "swarr;",                           // 2012
		    "swarrow;",                         // 2013
		    "swnwar;",                          // 2014
		    "szlig",                            // 2015
		    "szlig;",                           // 2016
		    "target;",                          // 2017
		    "tau;",                             // 2018
		    "tbrk;",                            // 2019
		    "tcaron;",                          // 2020
		    "tcedil;",                          // 2021
		    "tcy;",                             // 2022
		    "tdot;",                            // 2023
		    "telrec;",                          // 2024
		    "tfr;",                             // 2025
		    "there4;",                          // 2026
		    "therefore;",                       // 2027
		    "theta;",                           // 2028
		    "thetasym;",                        // 2029
		    "thetav;",                          // 2030
		    "thickapprox;",                     // 2031
		    "thicksim;",                        // 2032
		    "thinsp;",                          // 2033
		    "thkap;",                           // 2034
		    "thksim;",                          // 2035
		    "thorn",                            // 2036
		    "thorn;",                           // 2037
		    "tilde;",                           // 2038
		    "times",                            // 2039
		    "times;",                           // 2040
		    "timesb;",                          // 2041
		    "timesbar;",                        // 2042
		    "timesd;",                          // 2043
		    "tint;",                            // 2044
		    "toea;",                            // 2045
		    "top;",                             // 2046
		    "topbot;",                          // 2047
		    "topcir;",                          // 2048
		    "topf;",                            // 2049
		    "topfork;",                         // 2050
		    "tosa;",                            // 2051
		    "tprime;",                          // 2052
		    "trade;",                           // 2053
		    "triangle;",                        // 2054
		    "triangledown;",                    // 2055
		    "triangleleft;",                    // 2056
		    "trianglelefteq;",                  // 2057
		    "triangleq;",                       // 2058
		    "triangleright;",                   // 2059
		    "trianglerighteq;",                 // 2060
		    "tridot;",                          // 2061
		    "trie;",                            // 2062
		    "triminus;",                        // 2063
		    "triplus;",                         // 2064
		    "trisb;",                           // 2065
		    "tritime;",                         // 2066
		    "trpezium;",                        // 2067
		    "tscr;",                            // 2068
		    "tscy;",                            // 2069
		    "tshcy;",                           // 2070
		    "tstrok;",                          // 2071
		    "twixt;",                           // 2072
		    "twoheadleftarrow;",                // 2073
		    "twoheadrightarrow;",               // 2074
		    "uArr;",                            // 2075
		    "uHar;",                            // 2076
		    "uacute",                           // 2077
		    "uacute;",                          // 2078
		    "uarr;",                            // 2079
		    "ubrcy;",                           // 2080
		    "ubreve;",                          // 2081
		    "ucirc",                            // 2082
		    "ucirc;",                           // 2083
		    "ucy;",                             // 2084
		    "udarr;",                           // 2085
		    "udblac;",                          // 2086
		    "udhar;",                           // 2087
		    "ufisht;",                          // 2088
		    "ufr;",                             // 2089
		    "ugrave",                           // 2090
		    "ugrave;",                          // 2091
		    "uharl;",                           // 2092
		    "uharr;",                           // 2093
		    "uhblk;",                           // 2094
		    "ulcorn;",                          // 2095
		    "ulcorner;",                        // 2096
		    "ulcrop;",                          // 2097
		    "ultri;",                           // 2098
		    "umacr;",                           // 2099
		    "uml",                              // 2100
		    "uml;",                             // 2101
		    "uogon;",                           // 2102
		    "uopf;",                            // 2103
		    "uparrow;",                         // 2104
		    "updownarrow;",                     // 2105
		    "upharpoonleft;",                   // 2106
		    "upharpoonright;",                  // 2107
		    "uplus;",                           // 2108
		    "upsi;",                            // 2109
		    "upsih;",                           // 2110
		    "upsilon;",                         // 2111
		    "upuparrows;",                      // 2112
		    "urcorn;",                          // 2113
		    "urcorner;",                        // 2114
		    "urcrop;",                          // 2115
		    "uring;",                           // 2116
		    "urtri;",                           // 2117
		    "uscr;",                            // 2118
		    "utdot;",                           // 2119
		    "utilde;",                          // 2120
		    "utri;",                            // 2121
		    "utrif;",                           // 2122
		    "uuarr;",                           // 2123
		    "uuml",                             // 2124
		    "uuml;",                            // 2125
		    "uwangle;",                         // 2126
		    "vArr;",                            // 2127
		    "vBar;",                            // 2128
		    "vBarv;",                           // 2129
		    "vDash;",                           // 2130
		    "vangrt;",                          // 2131
		    "varepsilon;",                      // 2132
		    "varkappa;",                        // 2133
		    "varnothing;",                      // 2134
		    "varphi;",                          // 2135
		    "varpi;",                           // 2136
		    "varpropto;",                       // 2137
		    "varr;",                            // 2138
		    "varrho;",                          // 2139
		    "varsigma;",                        // 2140
		    "varsubsetneq;",                    // 2141
		    "varsubsetneqq;",                   // 2142
		    "varsupsetneq;",                    // 2143
		    "varsupsetneqq;",                   // 2144
		    "vartheta;",                        // 2145
		    "vartriangleleft;",                 // 2146
		    "vartriangleright;",                // 2147
		    "vcy;",                             // 2148
		    "vdash;",                           // 2149
		    "vee;",                             // 2150
		    "veebar;",                          // 2151
		    "veeeq;",                           // 2152
		    "vellip;",                          // 2153
		    "verbar;",                          // 2154
		    "vert;",                            // 2155
		    "vfr;",                             // 2156
		    "vltri;",                           // 2157
		    "vnsub;",                           // 2158
		    "vnsup;",                           // 2159
		    "vopf;",                            // 2160
		    "vprop;",                           // 2161
		    "vrtri;",                           // 2162
		    "vscr;",                            // 2163
		    "vsubnE;",                          // 2164
		    "vsubne;",                          // 2165
		    "vsupnE;",                          // 2166
		    "vsupne;",                          // 2167
		    "vzigzag;",                         // 2168
		    "wcirc;",                           // 2169
		    "wedbar;",                          // 2170
		    "wedge;",                           // 2171
		    "wedgeq;",                          // 2172
		    "weierp;",                          // 2173
		    "wfr;",                             // 2174
		    "wopf;",                            // 2175
		    "wp;",                              // 2176
		    "wr;",                              // 2177
		    "wreath;",                          // 2178
		    "wscr;",                            // 2179
		    "xcap;",                            // 2180
		    "xcirc;",                           // 2181
		    "xcup;",                            // 2182
		    "xdtri;",                           // 2183
		    "xfr;",                             // 2184
		    "xhArr;",                           // 2185
		    "xharr;",                           // 2186
		    "xi;",                              // 2187
		    "xlArr;",                           // 2188
		    "xlarr;",                           // 2189
		    "xmap;",                            // 2190
		    "xnis;",                            // 2191
		    "xodot;",                           // 2192
		    "xopf;",                            // 2193
		    "xoplus;",                          // 2194
		    "xotime;",                          // 2195
		    "xrArr;",                           // 2196
		    "xrarr;",                           // 2197
		    "xscr;",                            // 2198
		    "xsqcup;",                          // 2199
		    "xuplus;",                          // 2200
		    "xutri;",                           // 2201
		    "xvee;",                            // 2202
		    "xwedge;",                          // 2203
		    "yacute",                           // 2204
		    "yacute;",                          // 2205
		    "yacy;",                            // 2206
		    "ycirc;",                           // 2207
		    "ycy;",                             // 2208
		    "yen",                              // 2209
		    "yen;",                             // 2210
		    "yfr;",                             // 2211
		    "yicy;",                            // 2212
		    "yopf;",                            // 2213
		    "yscr;",                            // 2214
		    "yucy;",                            // 2215
		    "yuml",                             // 2216
		    "yuml;",                            // 2217
		    "zacute;",                          // 2218
		    "zcaron;",                          // 2219
		    "zcy;",                             // 2220
		    "zdot;",                            // 2221
		    "zeetrf;",                          // 2222
		    "zeta;",                            // 2223
		    "zfr;",                             // 2224
		    "zhcy;",                            // 2225
		    "zigrarr;",                         // 2226
		    "zopf;",                            // 2227
		    "zscr;",                            // 2228
		    "zwj;",                             // 2229
		    "zwnj;",                            // 2230
		};
	}

	static string_map_t<HTMLEscapeCodepoint> initialize_map() {
		string_map_t<HTMLEscapeCodepoint> mapped_strings;
		mapped_strings.reserve(2231);
		mapped_strings[string_t(owned_strings[0])] = {{198, 0}};
		mapped_strings[string_t(owned_strings[1])] = {{198, 0}};
		mapped_strings[string_t(owned_strings[2])] = {{38, 0}};
		mapped_strings[string_t(owned_strings[3])] = {{38, 0}};
		mapped_strings[string_t(owned_strings[4])] = {{193, 0}};
		mapped_strings[string_t(owned_strings[5])] = {{193, 0}};
		mapped_strings[string_t(owned_strings[6])] = {{258, 0}};
		mapped_strings[string_t(owned_strings[7])] = {{194, 0}};
		mapped_strings[string_t(owned_strings[8])] = {{194, 0}};
		mapped_strings[string_t(owned_strings[9])] = {{1040, 0}};
		mapped_strings[string_t(owned_strings[10])] = {{120068, 0}};
		mapped_strings[string_t(owned_strings[11])] = {{192, 0}};
		mapped_strings[string_t(owned_strings[12])] = {{192, 0}};
		mapped_strings[string_t(owned_strings[13])] = {{913, 0}};
		mapped_strings[string_t(owned_strings[14])] = {{256, 0}};
		mapped_strings[string_t(owned_strings[15])] = {{10835, 0}};
		mapped_strings[string_t(owned_strings[16])] = {{260, 0}};
		mapped_strings[string_t(owned_strings[17])] = {{120120, 0}};
		mapped_strings[string_t(owned_strings[18])] = {{8289, 0}};
		mapped_strings[string_t(owned_strings[19])] = {{197, 0}};
		mapped_strings[string_t(owned_strings[20])] = {{197, 0}};
		mapped_strings[string_t(owned_strings[21])] = {{119964, 0}};
		mapped_strings[string_t(owned_strings[22])] = {{8788, 0}};
		mapped_strings[string_t(owned_strings[23])] = {{195, 0}};
		mapped_strings[string_t(owned_strings[24])] = {{195, 0}};
		mapped_strings[string_t(owned_strings[25])] = {{196, 0}};
		mapped_strings[string_t(owned_strings[26])] = {{196, 0}};
		mapped_strings[string_t(owned_strings[27])] = {{8726, 0}};
		mapped_strings[string_t(owned_strings[28])] = {{10983, 0}};
		mapped_strings[string_t(owned_strings[29])] = {{8966, 0}};
		mapped_strings[string_t(owned_strings[30])] = {{1041, 0}};
		mapped_strings[string_t(owned_strings[31])] = {{8757, 0}};
		mapped_strings[string_t(owned_strings[32])] = {{8492, 0}};
		mapped_strings[string_t(owned_strings[33])] = {{914, 0}};
		mapped_strings[string_t(owned_strings[34])] = {{120069, 0}};
		mapped_strings[string_t(owned_strings[35])] = {{120121, 0}};
		mapped_strings[string_t(owned_strings[36])] = {{728, 0}};
		mapped_strings[string_t(owned_strings[37])] = {{8492, 0}};
		mapped_strings[string_t(owned_strings[38])] = {{8782, 0}};
		mapped_strings[string_t(owned_strings[39])] = {{1063, 0}};
		mapped_strings[string_t(owned_strings[40])] = {{169, 0}};
		mapped_strings[string_t(owned_strings[41])] = {{169, 0}};
		mapped_strings[string_t(owned_strings[42])] = {{262, 0}};
		mapped_strings[string_t(owned_strings[43])] = {{8914, 0}};
		mapped_strings[string_t(owned_strings[44])] = {{8517, 0}};
		mapped_strings[string_t(owned_strings[45])] = {{8493, 0}};
		mapped_strings[string_t(owned_strings[46])] = {{268, 0}};
		mapped_strings[string_t(owned_strings[47])] = {{199, 0}};
		mapped_strings[string_t(owned_strings[48])] = {{199, 0}};
		mapped_strings[string_t(owned_strings[49])] = {{264, 0}};
		mapped_strings[string_t(owned_strings[50])] = {{8752, 0}};
		mapped_strings[string_t(owned_strings[51])] = {{266, 0}};
		mapped_strings[string_t(owned_strings[52])] = {{184, 0}};
		mapped_strings[string_t(owned_strings[53])] = {{183, 0}};
		mapped_strings[string_t(owned_strings[54])] = {{8493, 0}};
		mapped_strings[string_t(owned_strings[55])] = {{935, 0}};
		mapped_strings[string_t(owned_strings[56])] = {{8857, 0}};
		mapped_strings[string_t(owned_strings[57])] = {{8854, 0}};
		mapped_strings[string_t(owned_strings[58])] = {{8853, 0}};
		mapped_strings[string_t(owned_strings[59])] = {{8855, 0}};
		mapped_strings[string_t(owned_strings[60])] = {{8754, 0}};
		mapped_strings[string_t(owned_strings[61])] = {{8221, 0}};
		mapped_strings[string_t(owned_strings[62])] = {{8217, 0}};
		mapped_strings[string_t(owned_strings[63])] = {{8759, 0}};
		mapped_strings[string_t(owned_strings[64])] = {{10868, 0}};
		mapped_strings[string_t(owned_strings[65])] = {{8801, 0}};
		mapped_strings[string_t(owned_strings[66])] = {{8751, 0}};
		mapped_strings[string_t(owned_strings[67])] = {{8750, 0}};
		mapped_strings[string_t(owned_strings[68])] = {{8450, 0}};
		mapped_strings[string_t(owned_strings[69])] = {{8720, 0}};
		mapped_strings[string_t(owned_strings[70])] = {{8755, 0}};
		mapped_strings[string_t(owned_strings[71])] = {{10799, 0}};
		mapped_strings[string_t(owned_strings[72])] = {{119966, 0}};
		mapped_strings[string_t(owned_strings[73])] = {{8915, 0}};
		mapped_strings[string_t(owned_strings[74])] = {{8781, 0}};
		mapped_strings[string_t(owned_strings[75])] = {{8517, 0}};
		mapped_strings[string_t(owned_strings[76])] = {{10513, 0}};
		mapped_strings[string_t(owned_strings[77])] = {{1026, 0}};
		mapped_strings[string_t(owned_strings[78])] = {{1029, 0}};
		mapped_strings[string_t(owned_strings[79])] = {{1039, 0}};
		mapped_strings[string_t(owned_strings[80])] = {{8225, 0}};
		mapped_strings[string_t(owned_strings[81])] = {{8609, 0}};
		mapped_strings[string_t(owned_strings[82])] = {{10980, 0}};
		mapped_strings[string_t(owned_strings[83])] = {{270, 0}};
		mapped_strings[string_t(owned_strings[84])] = {{1044, 0}};
		mapped_strings[string_t(owned_strings[85])] = {{8711, 0}};
		mapped_strings[string_t(owned_strings[86])] = {{916, 0}};
		mapped_strings[string_t(owned_strings[87])] = {{120071, 0}};
		mapped_strings[string_t(owned_strings[88])] = {{180, 0}};
		mapped_strings[string_t(owned_strings[89])] = {{729, 0}};
		mapped_strings[string_t(owned_strings[90])] = {{733, 0}};
		mapped_strings[string_t(owned_strings[91])] = {{96, 0}};
		mapped_strings[string_t(owned_strings[92])] = {{732, 0}};
		mapped_strings[string_t(owned_strings[93])] = {{8900, 0}};
		mapped_strings[string_t(owned_strings[94])] = {{8518, 0}};
		mapped_strings[string_t(owned_strings[95])] = {{120123, 0}};
		mapped_strings[string_t(owned_strings[96])] = {{168, 0}};
		mapped_strings[string_t(owned_strings[97])] = {{8412, 0}};
		mapped_strings[string_t(owned_strings[98])] = {{8784, 0}};
		mapped_strings[string_t(owned_strings[99])] = {{8751, 0}};
		mapped_strings[string_t(owned_strings[100])] = {{168, 0}};
		mapped_strings[string_t(owned_strings[101])] = {{8659, 0}};
		mapped_strings[string_t(owned_strings[102])] = {{8656, 0}};
		mapped_strings[string_t(owned_strings[103])] = {{8660, 0}};
		mapped_strings[string_t(owned_strings[104])] = {{10980, 0}};
		mapped_strings[string_t(owned_strings[105])] = {{10232, 0}};
		mapped_strings[string_t(owned_strings[106])] = {{10234, 0}};
		mapped_strings[string_t(owned_strings[107])] = {{10233, 0}};
		mapped_strings[string_t(owned_strings[108])] = {{8658, 0}};
		mapped_strings[string_t(owned_strings[109])] = {{8872, 0}};
		mapped_strings[string_t(owned_strings[110])] = {{8657, 0}};
		mapped_strings[string_t(owned_strings[111])] = {{8661, 0}};
		mapped_strings[string_t(owned_strings[112])] = {{8741, 0}};
		mapped_strings[string_t(owned_strings[113])] = {{8595, 0}};
		mapped_strings[string_t(owned_strings[114])] = {{10515, 0}};
		mapped_strings[string_t(owned_strings[115])] = {{8693, 0}};
		mapped_strings[string_t(owned_strings[116])] = {{785, 0}};
		mapped_strings[string_t(owned_strings[117])] = {{10576, 0}};
		mapped_strings[string_t(owned_strings[118])] = {{10590, 0}};
		mapped_strings[string_t(owned_strings[119])] = {{8637, 0}};
		mapped_strings[string_t(owned_strings[120])] = {{10582, 0}};
		mapped_strings[string_t(owned_strings[121])] = {{10591, 0}};
		mapped_strings[string_t(owned_strings[122])] = {{8641, 0}};
		mapped_strings[string_t(owned_strings[123])] = {{10583, 0}};
		mapped_strings[string_t(owned_strings[124])] = {{8868, 0}};
		mapped_strings[string_t(owned_strings[125])] = {{8615, 0}};
		mapped_strings[string_t(owned_strings[126])] = {{8659, 0}};
		mapped_strings[string_t(owned_strings[127])] = {{119967, 0}};
		mapped_strings[string_t(owned_strings[128])] = {{272, 0}};
		mapped_strings[string_t(owned_strings[129])] = {{330, 0}};
		mapped_strings[string_t(owned_strings[130])] = {{208, 0}};
		mapped_strings[string_t(owned_strings[131])] = {{208, 0}};
		mapped_strings[string_t(owned_strings[132])] = {{201, 0}};
		mapped_strings[string_t(owned_strings[133])] = {{201, 0}};
		mapped_strings[string_t(owned_strings[134])] = {{282, 0}};
		mapped_strings[string_t(owned_strings[135])] = {{202, 0}};
		mapped_strings[string_t(owned_strings[136])] = {{202, 0}};
		mapped_strings[string_t(owned_strings[137])] = {{1069, 0}};
		mapped_strings[string_t(owned_strings[138])] = {{278, 0}};
		mapped_strings[string_t(owned_strings[139])] = {{120072, 0}};
		mapped_strings[string_t(owned_strings[140])] = {{200, 0}};
		mapped_strings[string_t(owned_strings[141])] = {{200, 0}};
		mapped_strings[string_t(owned_strings[142])] = {{8712, 0}};
		mapped_strings[string_t(owned_strings[143])] = {{274, 0}};
		mapped_strings[string_t(owned_strings[144])] = {{9723, 0}};
		mapped_strings[string_t(owned_strings[145])] = {{9643, 0}};
		mapped_strings[string_t(owned_strings[146])] = {{280, 0}};
		mapped_strings[string_t(owned_strings[147])] = {{120124, 0}};
		mapped_strings[string_t(owned_strings[148])] = {{917, 0}};
		mapped_strings[string_t(owned_strings[149])] = {{10869, 0}};
		mapped_strings[string_t(owned_strings[150])] = {{8770, 0}};
		mapped_strings[string_t(owned_strings[151])] = {{8652, 0}};
		mapped_strings[string_t(owned_strings[152])] = {{8496, 0}};
		mapped_strings[string_t(owned_strings[153])] = {{10867, 0}};
		mapped_strings[string_t(owned_strings[154])] = {{919, 0}};
		mapped_strings[string_t(owned_strings[155])] = {{203, 0}};
		mapped_strings[string_t(owned_strings[156])] = {{203, 0}};
		mapped_strings[string_t(owned_strings[157])] = {{8707, 0}};
		mapped_strings[string_t(owned_strings[158])] = {{8519, 0}};
		mapped_strings[string_t(owned_strings[159])] = {{1060, 0}};
		mapped_strings[string_t(owned_strings[160])] = {{120073, 0}};
		mapped_strings[string_t(owned_strings[161])] = {{9724, 0}};
		mapped_strings[string_t(owned_strings[162])] = {{9642, 0}};
		mapped_strings[string_t(owned_strings[163])] = {{120125, 0}};
		mapped_strings[string_t(owned_strings[164])] = {{8704, 0}};
		mapped_strings[string_t(owned_strings[165])] = {{8497, 0}};
		mapped_strings[string_t(owned_strings[166])] = {{8497, 0}};
		mapped_strings[string_t(owned_strings[167])] = {{1027, 0}};
		mapped_strings[string_t(owned_strings[168])] = {{62, 0}};
		mapped_strings[string_t(owned_strings[169])] = {{62, 0}};
		mapped_strings[string_t(owned_strings[170])] = {{915, 0}};
		mapped_strings[string_t(owned_strings[171])] = {{988, 0}};
		mapped_strings[string_t(owned_strings[172])] = {{286, 0}};
		mapped_strings[string_t(owned_strings[173])] = {{290, 0}};
		mapped_strings[string_t(owned_strings[174])] = {{284, 0}};
		mapped_strings[string_t(owned_strings[175])] = {{1043, 0}};
		mapped_strings[string_t(owned_strings[176])] = {{288, 0}};
		mapped_strings[string_t(owned_strings[177])] = {{120074, 0}};
		mapped_strings[string_t(owned_strings[178])] = {{8921, 0}};
		mapped_strings[string_t(owned_strings[179])] = {{120126, 0}};
		mapped_strings[string_t(owned_strings[180])] = {{8805, 0}};
		mapped_strings[string_t(owned_strings[181])] = {{8923, 0}};
		mapped_strings[string_t(owned_strings[182])] = {{8807, 0}};
		mapped_strings[string_t(owned_strings[183])] = {{10914, 0}};
		mapped_strings[string_t(owned_strings[184])] = {{8823, 0}};
		mapped_strings[string_t(owned_strings[185])] = {{10878, 0}};
		mapped_strings[string_t(owned_strings[186])] = {{8819, 0}};
		mapped_strings[string_t(owned_strings[187])] = {{119970, 0}};
		mapped_strings[string_t(owned_strings[188])] = {{8811, 0}};
		mapped_strings[string_t(owned_strings[189])] = {{1066, 0}};
		mapped_strings[string_t(owned_strings[190])] = {{711, 0}};
		mapped_strings[string_t(owned_strings[191])] = {{94, 0}};
		mapped_strings[string_t(owned_strings[192])] = {{292, 0}};
		mapped_strings[string_t(owned_strings[193])] = {{8460, 0}};
		mapped_strings[string_t(owned_strings[194])] = {{8459, 0}};
		mapped_strings[string_t(owned_strings[195])] = {{8461, 0}};
		mapped_strings[string_t(owned_strings[196])] = {{9472, 0}};
		mapped_strings[string_t(owned_strings[197])] = {{8459, 0}};
		mapped_strings[string_t(owned_strings[198])] = {{294, 0}};
		mapped_strings[string_t(owned_strings[199])] = {{8782, 0}};
		mapped_strings[string_t(owned_strings[200])] = {{8783, 0}};
		mapped_strings[string_t(owned_strings[201])] = {{1045, 0}};
		mapped_strings[string_t(owned_strings[202])] = {{306, 0}};
		mapped_strings[string_t(owned_strings[203])] = {{1025, 0}};
		mapped_strings[string_t(owned_strings[204])] = {{205, 0}};
		mapped_strings[string_t(owned_strings[205])] = {{205, 0}};
		mapped_strings[string_t(owned_strings[206])] = {{206, 0}};
		mapped_strings[string_t(owned_strings[207])] = {{206, 0}};
		mapped_strings[string_t(owned_strings[208])] = {{1048, 0}};
		mapped_strings[string_t(owned_strings[209])] = {{304, 0}};
		mapped_strings[string_t(owned_strings[210])] = {{8465, 0}};
		mapped_strings[string_t(owned_strings[211])] = {{204, 0}};
		mapped_strings[string_t(owned_strings[212])] = {{204, 0}};
		mapped_strings[string_t(owned_strings[213])] = {{8465, 0}};
		mapped_strings[string_t(owned_strings[214])] = {{298, 0}};
		mapped_strings[string_t(owned_strings[215])] = {{8520, 0}};
		mapped_strings[string_t(owned_strings[216])] = {{8658, 0}};
		mapped_strings[string_t(owned_strings[217])] = {{8748, 0}};
		mapped_strings[string_t(owned_strings[218])] = {{8747, 0}};
		mapped_strings[string_t(owned_strings[219])] = {{8898, 0}};
		mapped_strings[string_t(owned_strings[220])] = {{8291, 0}};
		mapped_strings[string_t(owned_strings[221])] = {{8290, 0}};
		mapped_strings[string_t(owned_strings[222])] = {{302, 0}};
		mapped_strings[string_t(owned_strings[223])] = {{120128, 0}};
		mapped_strings[string_t(owned_strings[224])] = {{921, 0}};
		mapped_strings[string_t(owned_strings[225])] = {{8464, 0}};
		mapped_strings[string_t(owned_strings[226])] = {{296, 0}};
		mapped_strings[string_t(owned_strings[227])] = {{1030, 0}};
		mapped_strings[string_t(owned_strings[228])] = {{207, 0}};
		mapped_strings[string_t(owned_strings[229])] = {{207, 0}};
		mapped_strings[string_t(owned_strings[230])] = {{308, 0}};
		mapped_strings[string_t(owned_strings[231])] = {{1049, 0}};
		mapped_strings[string_t(owned_strings[232])] = {{120077, 0}};
		mapped_strings[string_t(owned_strings[233])] = {{120129, 0}};
		mapped_strings[string_t(owned_strings[234])] = {{119973, 0}};
		mapped_strings[string_t(owned_strings[235])] = {{1032, 0}};
		mapped_strings[string_t(owned_strings[236])] = {{1028, 0}};
		mapped_strings[string_t(owned_strings[237])] = {{1061, 0}};
		mapped_strings[string_t(owned_strings[238])] = {{1036, 0}};
		mapped_strings[string_t(owned_strings[239])] = {{922, 0}};
		mapped_strings[string_t(owned_strings[240])] = {{310, 0}};
		mapped_strings[string_t(owned_strings[241])] = {{1050, 0}};
		mapped_strings[string_t(owned_strings[242])] = {{120078, 0}};
		mapped_strings[string_t(owned_strings[243])] = {{120130, 0}};
		mapped_strings[string_t(owned_strings[244])] = {{119974, 0}};
		mapped_strings[string_t(owned_strings[245])] = {{1033, 0}};
		mapped_strings[string_t(owned_strings[246])] = {{60, 0}};
		mapped_strings[string_t(owned_strings[247])] = {{60, 0}};
		mapped_strings[string_t(owned_strings[248])] = {{313, 0}};
		mapped_strings[string_t(owned_strings[249])] = {{923, 0}};
		mapped_strings[string_t(owned_strings[250])] = {{10218, 0}};
		mapped_strings[string_t(owned_strings[251])] = {{8466, 0}};
		mapped_strings[string_t(owned_strings[252])] = {{8606, 0}};
		mapped_strings[string_t(owned_strings[253])] = {{317, 0}};
		mapped_strings[string_t(owned_strings[254])] = {{315, 0}};
		mapped_strings[string_t(owned_strings[255])] = {{1051, 0}};
		mapped_strings[string_t(owned_strings[256])] = {{10216, 0}};
		mapped_strings[string_t(owned_strings[257])] = {{8592, 0}};
		mapped_strings[string_t(owned_strings[258])] = {{8676, 0}};
		mapped_strings[string_t(owned_strings[259])] = {{8646, 0}};
		mapped_strings[string_t(owned_strings[260])] = {{8968, 0}};
		mapped_strings[string_t(owned_strings[261])] = {{10214, 0}};
		mapped_strings[string_t(owned_strings[262])] = {{10593, 0}};
		mapped_strings[string_t(owned_strings[263])] = {{8643, 0}};
		mapped_strings[string_t(owned_strings[264])] = {{10585, 0}};
		mapped_strings[string_t(owned_strings[265])] = {{8970, 0}};
		mapped_strings[string_t(owned_strings[266])] = {{8596, 0}};
		mapped_strings[string_t(owned_strings[267])] = {{10574, 0}};
		mapped_strings[string_t(owned_strings[268])] = {{8867, 0}};
		mapped_strings[string_t(owned_strings[269])] = {{8612, 0}};
		mapped_strings[string_t(owned_strings[270])] = {{10586, 0}};
		mapped_strings[string_t(owned_strings[271])] = {{8882, 0}};
		mapped_strings[string_t(owned_strings[272])] = {{10703, 0}};
		mapped_strings[string_t(owned_strings[273])] = {{8884, 0}};
		mapped_strings[string_t(owned_strings[274])] = {{10577, 0}};
		mapped_strings[string_t(owned_strings[275])] = {{10592, 0}};
		mapped_strings[string_t(owned_strings[276])] = {{8639, 0}};
		mapped_strings[string_t(owned_strings[277])] = {{10584, 0}};
		mapped_strings[string_t(owned_strings[278])] = {{8636, 0}};
		mapped_strings[string_t(owned_strings[279])] = {{10578, 0}};
		mapped_strings[string_t(owned_strings[280])] = {{8656, 0}};
		mapped_strings[string_t(owned_strings[281])] = {{8660, 0}};
		mapped_strings[string_t(owned_strings[282])] = {{8922, 0}};
		mapped_strings[string_t(owned_strings[283])] = {{8806, 0}};
		mapped_strings[string_t(owned_strings[284])] = {{8822, 0}};
		mapped_strings[string_t(owned_strings[285])] = {{10913, 0}};
		mapped_strings[string_t(owned_strings[286])] = {{10877, 0}};
		mapped_strings[string_t(owned_strings[287])] = {{8818, 0}};
		mapped_strings[string_t(owned_strings[288])] = {{120079, 0}};
		mapped_strings[string_t(owned_strings[289])] = {{8920, 0}};
		mapped_strings[string_t(owned_strings[290])] = {{8666, 0}};
		mapped_strings[string_t(owned_strings[291])] = {{319, 0}};
		mapped_strings[string_t(owned_strings[292])] = {{10229, 0}};
		mapped_strings[string_t(owned_strings[293])] = {{10231, 0}};
		mapped_strings[string_t(owned_strings[294])] = {{10230, 0}};
		mapped_strings[string_t(owned_strings[295])] = {{10232, 0}};
		mapped_strings[string_t(owned_strings[296])] = {{10234, 0}};
		mapped_strings[string_t(owned_strings[297])] = {{10233, 0}};
		mapped_strings[string_t(owned_strings[298])] = {{120131, 0}};
		mapped_strings[string_t(owned_strings[299])] = {{8601, 0}};
		mapped_strings[string_t(owned_strings[300])] = {{8600, 0}};
		mapped_strings[string_t(owned_strings[301])] = {{8466, 0}};
		mapped_strings[string_t(owned_strings[302])] = {{8624, 0}};
		mapped_strings[string_t(owned_strings[303])] = {{321, 0}};
		mapped_strings[string_t(owned_strings[304])] = {{8810, 0}};
		mapped_strings[string_t(owned_strings[305])] = {{10501, 0}};
		mapped_strings[string_t(owned_strings[306])] = {{1052, 0}};
		mapped_strings[string_t(owned_strings[307])] = {{8287, 0}};
		mapped_strings[string_t(owned_strings[308])] = {{8499, 0}};
		mapped_strings[string_t(owned_strings[309])] = {{120080, 0}};
		mapped_strings[string_t(owned_strings[310])] = {{8723, 0}};
		mapped_strings[string_t(owned_strings[311])] = {{120132, 0}};
		mapped_strings[string_t(owned_strings[312])] = {{8499, 0}};
		mapped_strings[string_t(owned_strings[313])] = {{924, 0}};
		mapped_strings[string_t(owned_strings[314])] = {{1034, 0}};
		mapped_strings[string_t(owned_strings[315])] = {{323, 0}};
		mapped_strings[string_t(owned_strings[316])] = {{327, 0}};
		mapped_strings[string_t(owned_strings[317])] = {{325, 0}};
		mapped_strings[string_t(owned_strings[318])] = {{1053, 0}};
		mapped_strings[string_t(owned_strings[319])] = {{8203, 0}};
		mapped_strings[string_t(owned_strings[320])] = {{8203, 0}};
		mapped_strings[string_t(owned_strings[321])] = {{8203, 0}};
		mapped_strings[string_t(owned_strings[322])] = {{8203, 0}};
		mapped_strings[string_t(owned_strings[323])] = {{8811, 0}};
		mapped_strings[string_t(owned_strings[324])] = {{8810, 0}};
		mapped_strings[string_t(owned_strings[325])] = {{10, 0}};
		mapped_strings[string_t(owned_strings[326])] = {{120081, 0}};
		mapped_strings[string_t(owned_strings[327])] = {{8288, 0}};
		mapped_strings[string_t(owned_strings[328])] = {{160, 0}};
		mapped_strings[string_t(owned_strings[329])] = {{8469, 0}};
		mapped_strings[string_t(owned_strings[330])] = {{10988, 0}};
		mapped_strings[string_t(owned_strings[331])] = {{8802, 0}};
		mapped_strings[string_t(owned_strings[332])] = {{8813, 0}};
		mapped_strings[string_t(owned_strings[333])] = {{8742, 0}};
		mapped_strings[string_t(owned_strings[334])] = {{8713, 0}};
		mapped_strings[string_t(owned_strings[335])] = {{8800, 0}};
		mapped_strings[string_t(owned_strings[336])] = {{8770, 824}};
		mapped_strings[string_t(owned_strings[337])] = {{8708, 0}};
		mapped_strings[string_t(owned_strings[338])] = {{8815, 0}};
		mapped_strings[string_t(owned_strings[339])] = {{8817, 0}};
		mapped_strings[string_t(owned_strings[340])] = {{8807, 824}};
		mapped_strings[string_t(owned_strings[341])] = {{8811, 824}};
		mapped_strings[string_t(owned_strings[342])] = {{8825, 0}};
		mapped_strings[string_t(owned_strings[343])] = {{10878, 824}};
		mapped_strings[string_t(owned_strings[344])] = {{8821, 0}};
		mapped_strings[string_t(owned_strings[345])] = {{8782, 824}};
		mapped_strings[string_t(owned_strings[346])] = {{8783, 824}};
		mapped_strings[string_t(owned_strings[347])] = {{8938, 0}};
		mapped_strings[string_t(owned_strings[348])] = {{10703, 824}};
		mapped_strings[string_t(owned_strings[349])] = {{8940, 0}};
		mapped_strings[string_t(owned_strings[350])] = {{8814, 0}};
		mapped_strings[string_t(owned_strings[351])] = {{8816, 0}};
		mapped_strings[string_t(owned_strings[352])] = {{8824, 0}};
		mapped_strings[string_t(owned_strings[353])] = {{8810, 824}};
		mapped_strings[string_t(owned_strings[354])] = {{10877, 824}};
		mapped_strings[string_t(owned_strings[355])] = {{8820, 0}};
		mapped_strings[string_t(owned_strings[356])] = {{10914, 824}};
		mapped_strings[string_t(owned_strings[357])] = {{10913, 824}};
		mapped_strings[string_t(owned_strings[358])] = {{8832, 0}};
		mapped_strings[string_t(owned_strings[359])] = {{10927, 824}};
		mapped_strings[string_t(owned_strings[360])] = {{8928, 0}};
		mapped_strings[string_t(owned_strings[361])] = {{8716, 0}};
		mapped_strings[string_t(owned_strings[362])] = {{8939, 0}};
		mapped_strings[string_t(owned_strings[363])] = {{10704, 824}};
		mapped_strings[string_t(owned_strings[364])] = {{8941, 0}};
		mapped_strings[string_t(owned_strings[365])] = {{8847, 824}};
		mapped_strings[string_t(owned_strings[366])] = {{8930, 0}};
		mapped_strings[string_t(owned_strings[367])] = {{8848, 824}};
		mapped_strings[string_t(owned_strings[368])] = {{8931, 0}};
		mapped_strings[string_t(owned_strings[369])] = {{8834, 8402}};
		mapped_strings[string_t(owned_strings[370])] = {{8840, 0}};
		mapped_strings[string_t(owned_strings[371])] = {{8833, 0}};
		mapped_strings[string_t(owned_strings[372])] = {{10928, 824}};
		mapped_strings[string_t(owned_strings[373])] = {{8929, 0}};
		mapped_strings[string_t(owned_strings[374])] = {{8831, 824}};
		mapped_strings[string_t(owned_strings[375])] = {{8835, 8402}};
		mapped_strings[string_t(owned_strings[376])] = {{8841, 0}};
		mapped_strings[string_t(owned_strings[377])] = {{8769, 0}};
		mapped_strings[string_t(owned_strings[378])] = {{8772, 0}};
		mapped_strings[string_t(owned_strings[379])] = {{8775, 0}};
		mapped_strings[string_t(owned_strings[380])] = {{8777, 0}};
		mapped_strings[string_t(owned_strings[381])] = {{8740, 0}};
		mapped_strings[string_t(owned_strings[382])] = {{119977, 0}};
		mapped_strings[string_t(owned_strings[383])] = {{209, 0}};
		mapped_strings[string_t(owned_strings[384])] = {{209, 0}};
		mapped_strings[string_t(owned_strings[385])] = {{925, 0}};
		mapped_strings[string_t(owned_strings[386])] = {{338, 0}};
		mapped_strings[string_t(owned_strings[387])] = {{211, 0}};
		mapped_strings[string_t(owned_strings[388])] = {{211, 0}};
		mapped_strings[string_t(owned_strings[389])] = {{212, 0}};
		mapped_strings[string_t(owned_strings[390])] = {{212, 0}};
		mapped_strings[string_t(owned_strings[391])] = {{1054, 0}};
		mapped_strings[string_t(owned_strings[392])] = {{336, 0}};
		mapped_strings[string_t(owned_strings[393])] = {{120082, 0}};
		mapped_strings[string_t(owned_strings[394])] = {{210, 0}};
		mapped_strings[string_t(owned_strings[395])] = {{210, 0}};
		mapped_strings[string_t(owned_strings[396])] = {{332, 0}};
		mapped_strings[string_t(owned_strings[397])] = {{937, 0}};
		mapped_strings[string_t(owned_strings[398])] = {{927, 0}};
		mapped_strings[string_t(owned_strings[399])] = {{120134, 0}};
		mapped_strings[string_t(owned_strings[400])] = {{8220, 0}};
		mapped_strings[string_t(owned_strings[401])] = {{8216, 0}};
		mapped_strings[string_t(owned_strings[402])] = {{10836, 0}};
		mapped_strings[string_t(owned_strings[403])] = {{119978, 0}};
		mapped_strings[string_t(owned_strings[404])] = {{216, 0}};
		mapped_strings[string_t(owned_strings[405])] = {{216, 0}};
		mapped_strings[string_t(owned_strings[406])] = {{213, 0}};
		mapped_strings[string_t(owned_strings[407])] = {{213, 0}};
		mapped_strings[string_t(owned_strings[408])] = {{10807, 0}};
		mapped_strings[string_t(owned_strings[409])] = {{214, 0}};
		mapped_strings[string_t(owned_strings[410])] = {{214, 0}};
		mapped_strings[string_t(owned_strings[411])] = {{8254, 0}};
		mapped_strings[string_t(owned_strings[412])] = {{9182, 0}};
		mapped_strings[string_t(owned_strings[413])] = {{9140, 0}};
		mapped_strings[string_t(owned_strings[414])] = {{9180, 0}};
		mapped_strings[string_t(owned_strings[415])] = {{8706, 0}};
		mapped_strings[string_t(owned_strings[416])] = {{1055, 0}};
		mapped_strings[string_t(owned_strings[417])] = {{120083, 0}};
		mapped_strings[string_t(owned_strings[418])] = {{934, 0}};
		mapped_strings[string_t(owned_strings[419])] = {{928, 0}};
		mapped_strings[string_t(owned_strings[420])] = {{177, 0}};
		mapped_strings[string_t(owned_strings[421])] = {{8460, 0}};
		mapped_strings[string_t(owned_strings[422])] = {{8473, 0}};
		mapped_strings[string_t(owned_strings[423])] = {{10939, 0}};
		mapped_strings[string_t(owned_strings[424])] = {{8826, 0}};
		mapped_strings[string_t(owned_strings[425])] = {{10927, 0}};
		mapped_strings[string_t(owned_strings[426])] = {{8828, 0}};
		mapped_strings[string_t(owned_strings[427])] = {{8830, 0}};
		mapped_strings[string_t(owned_strings[428])] = {{8243, 0}};
		mapped_strings[string_t(owned_strings[429])] = {{8719, 0}};
		mapped_strings[string_t(owned_strings[430])] = {{8759, 0}};
		mapped_strings[string_t(owned_strings[431])] = {{8733, 0}};
		mapped_strings[string_t(owned_strings[432])] = {{119979, 0}};
		mapped_strings[string_t(owned_strings[433])] = {{936, 0}};
		mapped_strings[string_t(owned_strings[434])] = {{34, 0}};
		mapped_strings[string_t(owned_strings[435])] = {{34, 0}};
		mapped_strings[string_t(owned_strings[436])] = {{120084, 0}};
		mapped_strings[string_t(owned_strings[437])] = {{8474, 0}};
		mapped_strings[string_t(owned_strings[438])] = {{119980, 0}};
		mapped_strings[string_t(owned_strings[439])] = {{10512, 0}};
		mapped_strings[string_t(owned_strings[440])] = {{174, 0}};
		mapped_strings[string_t(owned_strings[441])] = {{174, 0}};
		mapped_strings[string_t(owned_strings[442])] = {{340, 0}};
		mapped_strings[string_t(owned_strings[443])] = {{10219, 0}};
		mapped_strings[string_t(owned_strings[444])] = {{8608, 0}};
		mapped_strings[string_t(owned_strings[445])] = {{10518, 0}};
		mapped_strings[string_t(owned_strings[446])] = {{344, 0}};
		mapped_strings[string_t(owned_strings[447])] = {{342, 0}};
		mapped_strings[string_t(owned_strings[448])] = {{1056, 0}};
		mapped_strings[string_t(owned_strings[449])] = {{8476, 0}};
		mapped_strings[string_t(owned_strings[450])] = {{8715, 0}};
		mapped_strings[string_t(owned_strings[451])] = {{8651, 0}};
		mapped_strings[string_t(owned_strings[452])] = {{10607, 0}};
		mapped_strings[string_t(owned_strings[453])] = {{8476, 0}};
		mapped_strings[string_t(owned_strings[454])] = {{929, 0}};
		mapped_strings[string_t(owned_strings[455])] = {{10217, 0}};
		mapped_strings[string_t(owned_strings[456])] = {{8594, 0}};
		mapped_strings[string_t(owned_strings[457])] = {{8677, 0}};
		mapped_strings[string_t(owned_strings[458])] = {{8644, 0}};
		mapped_strings[string_t(owned_strings[459])] = {{8969, 0}};
		mapped_strings[string_t(owned_strings[460])] = {{10215, 0}};
		mapped_strings[string_t(owned_strings[461])] = {{10589, 0}};
		mapped_strings[string_t(owned_strings[462])] = {{8642, 0}};
		mapped_strings[string_t(owned_strings[463])] = {{10581, 0}};
		mapped_strings[string_t(owned_strings[464])] = {{8971, 0}};
		mapped_strings[string_t(owned_strings[465])] = {{8866, 0}};
		mapped_strings[string_t(owned_strings[466])] = {{8614, 0}};
		mapped_strings[string_t(owned_strings[467])] = {{10587, 0}};
		mapped_strings[string_t(owned_strings[468])] = {{8883, 0}};
		mapped_strings[string_t(owned_strings[469])] = {{10704, 0}};
		mapped_strings[string_t(owned_strings[470])] = {{8885, 0}};
		mapped_strings[string_t(owned_strings[471])] = {{10575, 0}};
		mapped_strings[string_t(owned_strings[472])] = {{10588, 0}};
		mapped_strings[string_t(owned_strings[473])] = {{8638, 0}};
		mapped_strings[string_t(owned_strings[474])] = {{10580, 0}};
		mapped_strings[string_t(owned_strings[475])] = {{8640, 0}};
		mapped_strings[string_t(owned_strings[476])] = {{10579, 0}};
		mapped_strings[string_t(owned_strings[477])] = {{8658, 0}};
		mapped_strings[string_t(owned_strings[478])] = {{8477, 0}};
		mapped_strings[string_t(owned_strings[479])] = {{10608, 0}};
		mapped_strings[string_t(owned_strings[480])] = {{8667, 0}};
		mapped_strings[string_t(owned_strings[481])] = {{8475, 0}};
		mapped_strings[string_t(owned_strings[482])] = {{8625, 0}};
		mapped_strings[string_t(owned_strings[483])] = {{10740, 0}};
		mapped_strings[string_t(owned_strings[484])] = {{1065, 0}};
		mapped_strings[string_t(owned_strings[485])] = {{1064, 0}};
		mapped_strings[string_t(owned_strings[486])] = {{1068, 0}};
		mapped_strings[string_t(owned_strings[487])] = {{346, 0}};
		mapped_strings[string_t(owned_strings[488])] = {{10940, 0}};
		mapped_strings[string_t(owned_strings[489])] = {{352, 0}};
		mapped_strings[string_t(owned_strings[490])] = {{350, 0}};
		mapped_strings[string_t(owned_strings[491])] = {{348, 0}};
		mapped_strings[string_t(owned_strings[492])] = {{1057, 0}};
		mapped_strings[string_t(owned_strings[493])] = {{120086, 0}};
		mapped_strings[string_t(owned_strings[494])] = {{8595, 0}};
		mapped_strings[string_t(owned_strings[495])] = {{8592, 0}};
		mapped_strings[string_t(owned_strings[496])] = {{8594, 0}};
		mapped_strings[string_t(owned_strings[497])] = {{8593, 0}};
		mapped_strings[string_t(owned_strings[498])] = {{931, 0}};
		mapped_strings[string_t(owned_strings[499])] = {{8728, 0}};
		mapped_strings[string_t(owned_strings[500])] = {{120138, 0}};
		mapped_strings[string_t(owned_strings[501])] = {{8730, 0}};
		mapped_strings[string_t(owned_strings[502])] = {{9633, 0}};
		mapped_strings[string_t(owned_strings[503])] = {{8851, 0}};
		mapped_strings[string_t(owned_strings[504])] = {{8847, 0}};
		mapped_strings[string_t(owned_strings[505])] = {{8849, 0}};
		mapped_strings[string_t(owned_strings[506])] = {{8848, 0}};
		mapped_strings[string_t(owned_strings[507])] = {{8850, 0}};
		mapped_strings[string_t(owned_strings[508])] = {{8852, 0}};
		mapped_strings[string_t(owned_strings[509])] = {{119982, 0}};
		mapped_strings[string_t(owned_strings[510])] = {{8902, 0}};
		mapped_strings[string_t(owned_strings[511])] = {{8912, 0}};
		mapped_strings[string_t(owned_strings[512])] = {{8912, 0}};
		mapped_strings[string_t(owned_strings[513])] = {{8838, 0}};
		mapped_strings[string_t(owned_strings[514])] = {{8827, 0}};
		mapped_strings[string_t(owned_strings[515])] = {{10928, 0}};
		mapped_strings[string_t(owned_strings[516])] = {{8829, 0}};
		mapped_strings[string_t(owned_strings[517])] = {{8831, 0}};
		mapped_strings[string_t(owned_strings[518])] = {{8715, 0}};
		mapped_strings[string_t(owned_strings[519])] = {{8721, 0}};
		mapped_strings[string_t(owned_strings[520])] = {{8913, 0}};
		mapped_strings[string_t(owned_strings[521])] = {{8835, 0}};
		mapped_strings[string_t(owned_strings[522])] = {{8839, 0}};
		mapped_strings[string_t(owned_strings[523])] = {{8913, 0}};
		mapped_strings[string_t(owned_strings[524])] = {{222, 0}};
		mapped_strings[string_t(owned_strings[525])] = {{222, 0}};
		mapped_strings[string_t(owned_strings[526])] = {{8482, 0}};
		mapped_strings[string_t(owned_strings[527])] = {{1035, 0}};
		mapped_strings[string_t(owned_strings[528])] = {{1062, 0}};
		mapped_strings[string_t(owned_strings[529])] = {{9, 0}};
		mapped_strings[string_t(owned_strings[530])] = {{932, 0}};
		mapped_strings[string_t(owned_strings[531])] = {{356, 0}};
		mapped_strings[string_t(owned_strings[532])] = {{354, 0}};
		mapped_strings[string_t(owned_strings[533])] = {{1058, 0}};
		mapped_strings[string_t(owned_strings[534])] = {{120087, 0}};
		mapped_strings[string_t(owned_strings[535])] = {{8756, 0}};
		mapped_strings[string_t(owned_strings[536])] = {{920, 0}};
		mapped_strings[string_t(owned_strings[537])] = {{8287, 8202}};
		mapped_strings[string_t(owned_strings[538])] = {{8201, 0}};
		mapped_strings[string_t(owned_strings[539])] = {{8764, 0}};
		mapped_strings[string_t(owned_strings[540])] = {{8771, 0}};
		mapped_strings[string_t(owned_strings[541])] = {{8773, 0}};
		mapped_strings[string_t(owned_strings[542])] = {{8776, 0}};
		mapped_strings[string_t(owned_strings[543])] = {{120139, 0}};
		mapped_strings[string_t(owned_strings[544])] = {{8411, 0}};
		mapped_strings[string_t(owned_strings[545])] = {{119983, 0}};
		mapped_strings[string_t(owned_strings[546])] = {{358, 0}};
		mapped_strings[string_t(owned_strings[547])] = {{218, 0}};
		mapped_strings[string_t(owned_strings[548])] = {{218, 0}};
		mapped_strings[string_t(owned_strings[549])] = {{8607, 0}};
		mapped_strings[string_t(owned_strings[550])] = {{10569, 0}};
		mapped_strings[string_t(owned_strings[551])] = {{1038, 0}};
		mapped_strings[string_t(owned_strings[552])] = {{364, 0}};
		mapped_strings[string_t(owned_strings[553])] = {{219, 0}};
		mapped_strings[string_t(owned_strings[554])] = {{219, 0}};
		mapped_strings[string_t(owned_strings[555])] = {{1059, 0}};
		mapped_strings[string_t(owned_strings[556])] = {{368, 0}};
		mapped_strings[string_t(owned_strings[557])] = {{120088, 0}};
		mapped_strings[string_t(owned_strings[558])] = {{217, 0}};
		mapped_strings[string_t(owned_strings[559])] = {{217, 0}};
		mapped_strings[string_t(owned_strings[560])] = {{362, 0}};
		mapped_strings[string_t(owned_strings[561])] = {{95, 0}};
		mapped_strings[string_t(owned_strings[562])] = {{9183, 0}};
		mapped_strings[string_t(owned_strings[563])] = {{9141, 0}};
		mapped_strings[string_t(owned_strings[564])] = {{9181, 0}};
		mapped_strings[string_t(owned_strings[565])] = {{8899, 0}};
		mapped_strings[string_t(owned_strings[566])] = {{8846, 0}};
		mapped_strings[string_t(owned_strings[567])] = {{370, 0}};
		mapped_strings[string_t(owned_strings[568])] = {{120140, 0}};
		mapped_strings[string_t(owned_strings[569])] = {{8593, 0}};
		mapped_strings[string_t(owned_strings[570])] = {{10514, 0}};
		mapped_strings[string_t(owned_strings[571])] = {{8645, 0}};
		mapped_strings[string_t(owned_strings[572])] = {{8597, 0}};
		mapped_strings[string_t(owned_strings[573])] = {{10606, 0}};
		mapped_strings[string_t(owned_strings[574])] = {{8869, 0}};
		mapped_strings[string_t(owned_strings[575])] = {{8613, 0}};
		mapped_strings[string_t(owned_strings[576])] = {{8657, 0}};
		mapped_strings[string_t(owned_strings[577])] = {{8661, 0}};
		mapped_strings[string_t(owned_strings[578])] = {{8598, 0}};
		mapped_strings[string_t(owned_strings[579])] = {{8599, 0}};
		mapped_strings[string_t(owned_strings[580])] = {{978, 0}};
		mapped_strings[string_t(owned_strings[581])] = {{933, 0}};
		mapped_strings[string_t(owned_strings[582])] = {{366, 0}};
		mapped_strings[string_t(owned_strings[583])] = {{119984, 0}};
		mapped_strings[string_t(owned_strings[584])] = {{360, 0}};
		mapped_strings[string_t(owned_strings[585])] = {{220, 0}};
		mapped_strings[string_t(owned_strings[586])] = {{220, 0}};
		mapped_strings[string_t(owned_strings[587])] = {{8875, 0}};
		mapped_strings[string_t(owned_strings[588])] = {{10987, 0}};
		mapped_strings[string_t(owned_strings[589])] = {{1042, 0}};
		mapped_strings[string_t(owned_strings[590])] = {{8873, 0}};
		mapped_strings[string_t(owned_strings[591])] = {{10982, 0}};
		mapped_strings[string_t(owned_strings[592])] = {{8897, 0}};
		mapped_strings[string_t(owned_strings[593])] = {{8214, 0}};
		mapped_strings[string_t(owned_strings[594])] = {{8214, 0}};
		mapped_strings[string_t(owned_strings[595])] = {{8739, 0}};
		mapped_strings[string_t(owned_strings[596])] = {{124, 0}};
		mapped_strings[string_t(owned_strings[597])] = {{10072, 0}};
		mapped_strings[string_t(owned_strings[598])] = {{8768, 0}};
		mapped_strings[string_t(owned_strings[599])] = {{8202, 0}};
		mapped_strings[string_t(owned_strings[600])] = {{120089, 0}};
		mapped_strings[string_t(owned_strings[601])] = {{120141, 0}};
		mapped_strings[string_t(owned_strings[602])] = {{119985, 0}};
		mapped_strings[string_t(owned_strings[603])] = {{8874, 0}};
		mapped_strings[string_t(owned_strings[604])] = {{372, 0}};
		mapped_strings[string_t(owned_strings[605])] = {{8896, 0}};
		mapped_strings[string_t(owned_strings[606])] = {{120090, 0}};
		mapped_strings[string_t(owned_strings[607])] = {{120142, 0}};
		mapped_strings[string_t(owned_strings[608])] = {{119986, 0}};
		mapped_strings[string_t(owned_strings[609])] = {{120091, 0}};
		mapped_strings[string_t(owned_strings[610])] = {{926, 0}};
		mapped_strings[string_t(owned_strings[611])] = {{120143, 0}};
		mapped_strings[string_t(owned_strings[612])] = {{119987, 0}};
		mapped_strings[string_t(owned_strings[613])] = {{1071, 0}};
		mapped_strings[string_t(owned_strings[614])] = {{1031, 0}};
		mapped_strings[string_t(owned_strings[615])] = {{1070, 0}};
		mapped_strings[string_t(owned_strings[616])] = {{221, 0}};
		mapped_strings[string_t(owned_strings[617])] = {{221, 0}};
		mapped_strings[string_t(owned_strings[618])] = {{374, 0}};
		mapped_strings[string_t(owned_strings[619])] = {{1067, 0}};
		mapped_strings[string_t(owned_strings[620])] = {{120092, 0}};
		mapped_strings[string_t(owned_strings[621])] = {{120144, 0}};
		mapped_strings[string_t(owned_strings[622])] = {{119988, 0}};
		mapped_strings[string_t(owned_strings[623])] = {{376, 0}};
		mapped_strings[string_t(owned_strings[624])] = {{1046, 0}};
		mapped_strings[string_t(owned_strings[625])] = {{377, 0}};
		mapped_strings[string_t(owned_strings[626])] = {{381, 0}};
		mapped_strings[string_t(owned_strings[627])] = {{1047, 0}};
		mapped_strings[string_t(owned_strings[628])] = {{379, 0}};
		mapped_strings[string_t(owned_strings[629])] = {{8203, 0}};
		mapped_strings[string_t(owned_strings[630])] = {{918, 0}};
		mapped_strings[string_t(owned_strings[631])] = {{8488, 0}};
		mapped_strings[string_t(owned_strings[632])] = {{8484, 0}};
		mapped_strings[string_t(owned_strings[633])] = {{119989, 0}};
		mapped_strings[string_t(owned_strings[634])] = {{225, 0}};
		mapped_strings[string_t(owned_strings[635])] = {{225, 0}};
		mapped_strings[string_t(owned_strings[636])] = {{259, 0}};
		mapped_strings[string_t(owned_strings[637])] = {{8766, 0}};
		mapped_strings[string_t(owned_strings[638])] = {{8766, 819}};
		mapped_strings[string_t(owned_strings[639])] = {{8767, 0}};
		mapped_strings[string_t(owned_strings[640])] = {{226, 0}};
		mapped_strings[string_t(owned_strings[641])] = {{226, 0}};
		mapped_strings[string_t(owned_strings[642])] = {{180, 0}};
		mapped_strings[string_t(owned_strings[643])] = {{180, 0}};
		mapped_strings[string_t(owned_strings[644])] = {{1072, 0}};
		mapped_strings[string_t(owned_strings[645])] = {{230, 0}};
		mapped_strings[string_t(owned_strings[646])] = {{230, 0}};
		mapped_strings[string_t(owned_strings[647])] = {{8289, 0}};
		mapped_strings[string_t(owned_strings[648])] = {{120094, 0}};
		mapped_strings[string_t(owned_strings[649])] = {{224, 0}};
		mapped_strings[string_t(owned_strings[650])] = {{224, 0}};
		mapped_strings[string_t(owned_strings[651])] = {{8501, 0}};
		mapped_strings[string_t(owned_strings[652])] = {{8501, 0}};
		mapped_strings[string_t(owned_strings[653])] = {{945, 0}};
		mapped_strings[string_t(owned_strings[654])] = {{257, 0}};
		mapped_strings[string_t(owned_strings[655])] = {{10815, 0}};
		mapped_strings[string_t(owned_strings[656])] = {{38, 0}};
		mapped_strings[string_t(owned_strings[657])] = {{38, 0}};
		mapped_strings[string_t(owned_strings[658])] = {{8743, 0}};
		mapped_strings[string_t(owned_strings[659])] = {{10837, 0}};
		mapped_strings[string_t(owned_strings[660])] = {{10844, 0}};
		mapped_strings[string_t(owned_strings[661])] = {{10840, 0}};
		mapped_strings[string_t(owned_strings[662])] = {{10842, 0}};
		mapped_strings[string_t(owned_strings[663])] = {{8736, 0}};
		mapped_strings[string_t(owned_strings[664])] = {{10660, 0}};
		mapped_strings[string_t(owned_strings[665])] = {{8736, 0}};
		mapped_strings[string_t(owned_strings[666])] = {{8737, 0}};
		mapped_strings[string_t(owned_strings[667])] = {{10664, 0}};
		mapped_strings[string_t(owned_strings[668])] = {{10665, 0}};
		mapped_strings[string_t(owned_strings[669])] = {{10666, 0}};
		mapped_strings[string_t(owned_strings[670])] = {{10667, 0}};
		mapped_strings[string_t(owned_strings[671])] = {{10668, 0}};
		mapped_strings[string_t(owned_strings[672])] = {{10669, 0}};
		mapped_strings[string_t(owned_strings[673])] = {{10670, 0}};
		mapped_strings[string_t(owned_strings[674])] = {{10671, 0}};
		mapped_strings[string_t(owned_strings[675])] = {{8735, 0}};
		mapped_strings[string_t(owned_strings[676])] = {{8894, 0}};
		mapped_strings[string_t(owned_strings[677])] = {{10653, 0}};
		mapped_strings[string_t(owned_strings[678])] = {{8738, 0}};
		mapped_strings[string_t(owned_strings[679])] = {{197, 0}};
		mapped_strings[string_t(owned_strings[680])] = {{9084, 0}};
		mapped_strings[string_t(owned_strings[681])] = {{261, 0}};
		mapped_strings[string_t(owned_strings[682])] = {{120146, 0}};
		mapped_strings[string_t(owned_strings[683])] = {{8776, 0}};
		mapped_strings[string_t(owned_strings[684])] = {{10864, 0}};
		mapped_strings[string_t(owned_strings[685])] = {{10863, 0}};
		mapped_strings[string_t(owned_strings[686])] = {{8778, 0}};
		mapped_strings[string_t(owned_strings[687])] = {{8779, 0}};
		mapped_strings[string_t(owned_strings[688])] = {{39, 0}};
		mapped_strings[string_t(owned_strings[689])] = {{8776, 0}};
		mapped_strings[string_t(owned_strings[690])] = {{8778, 0}};
		mapped_strings[string_t(owned_strings[691])] = {{229, 0}};
		mapped_strings[string_t(owned_strings[692])] = {{229, 0}};
		mapped_strings[string_t(owned_strings[693])] = {{119990, 0}};
		mapped_strings[string_t(owned_strings[694])] = {{42, 0}};
		mapped_strings[string_t(owned_strings[695])] = {{8776, 0}};
		mapped_strings[string_t(owned_strings[696])] = {{8781, 0}};
		mapped_strings[string_t(owned_strings[697])] = {{227, 0}};
		mapped_strings[string_t(owned_strings[698])] = {{227, 0}};
		mapped_strings[string_t(owned_strings[699])] = {{228, 0}};
		mapped_strings[string_t(owned_strings[700])] = {{228, 0}};
		mapped_strings[string_t(owned_strings[701])] = {{8755, 0}};
		mapped_strings[string_t(owned_strings[702])] = {{10769, 0}};
		mapped_strings[string_t(owned_strings[703])] = {{10989, 0}};
		mapped_strings[string_t(owned_strings[704])] = {{8780, 0}};
		mapped_strings[string_t(owned_strings[705])] = {{1014, 0}};
		mapped_strings[string_t(owned_strings[706])] = {{8245, 0}};
		mapped_strings[string_t(owned_strings[707])] = {{8765, 0}};
		mapped_strings[string_t(owned_strings[708])] = {{8909, 0}};
		mapped_strings[string_t(owned_strings[709])] = {{8893, 0}};
		mapped_strings[string_t(owned_strings[710])] = {{8965, 0}};
		mapped_strings[string_t(owned_strings[711])] = {{8965, 0}};
		mapped_strings[string_t(owned_strings[712])] = {{9141, 0}};
		mapped_strings[string_t(owned_strings[713])] = {{9142, 0}};
		mapped_strings[string_t(owned_strings[714])] = {{8780, 0}};
		mapped_strings[string_t(owned_strings[715])] = {{1073, 0}};
		mapped_strings[string_t(owned_strings[716])] = {{8222, 0}};
		mapped_strings[string_t(owned_strings[717])] = {{8757, 0}};
		mapped_strings[string_t(owned_strings[718])] = {{8757, 0}};
		mapped_strings[string_t(owned_strings[719])] = {{10672, 0}};
		mapped_strings[string_t(owned_strings[720])] = {{1014, 0}};
		mapped_strings[string_t(owned_strings[721])] = {{8492, 0}};
		mapped_strings[string_t(owned_strings[722])] = {{946, 0}};
		mapped_strings[string_t(owned_strings[723])] = {{8502, 0}};
		mapped_strings[string_t(owned_strings[724])] = {{8812, 0}};
		mapped_strings[string_t(owned_strings[725])] = {{120095, 0}};
		mapped_strings[string_t(owned_strings[726])] = {{8898, 0}};
		mapped_strings[string_t(owned_strings[727])] = {{9711, 0}};
		mapped_strings[string_t(owned_strings[728])] = {{8899, 0}};
		mapped_strings[string_t(owned_strings[729])] = {{10752, 0}};
		mapped_strings[string_t(owned_strings[730])] = {{10753, 0}};
		mapped_strings[string_t(owned_strings[731])] = {{10754, 0}};
		mapped_strings[string_t(owned_strings[732])] = {{10758, 0}};
		mapped_strings[string_t(owned_strings[733])] = {{9733, 0}};
		mapped_strings[string_t(owned_strings[734])] = {{9661, 0}};
		mapped_strings[string_t(owned_strings[735])] = {{9651, 0}};
		mapped_strings[string_t(owned_strings[736])] = {{10756, 0}};
		mapped_strings[string_t(owned_strings[737])] = {{8897, 0}};
		mapped_strings[string_t(owned_strings[738])] = {{8896, 0}};
		mapped_strings[string_t(owned_strings[739])] = {{10509, 0}};
		mapped_strings[string_t(owned_strings[740])] = {{10731, 0}};
		mapped_strings[string_t(owned_strings[741])] = {{9642, 0}};
		mapped_strings[string_t(owned_strings[742])] = {{9652, 0}};
		mapped_strings[string_t(owned_strings[743])] = {{9662, 0}};
		mapped_strings[string_t(owned_strings[744])] = {{9666, 0}};
		mapped_strings[string_t(owned_strings[745])] = {{9656, 0}};
		mapped_strings[string_t(owned_strings[746])] = {{9251, 0}};
		mapped_strings[string_t(owned_strings[747])] = {{9618, 0}};
		mapped_strings[string_t(owned_strings[748])] = {{9617, 0}};
		mapped_strings[string_t(owned_strings[749])] = {{9619, 0}};
		mapped_strings[string_t(owned_strings[750])] = {{9608, 0}};
		mapped_strings[string_t(owned_strings[751])] = {{61, 8421}};
		mapped_strings[string_t(owned_strings[752])] = {{8801, 8421}};
		mapped_strings[string_t(owned_strings[753])] = {{8976, 0}};
		mapped_strings[string_t(owned_strings[754])] = {{120147, 0}};
		mapped_strings[string_t(owned_strings[755])] = {{8869, 0}};
		mapped_strings[string_t(owned_strings[756])] = {{8869, 0}};
		mapped_strings[string_t(owned_strings[757])] = {{8904, 0}};
		mapped_strings[string_t(owned_strings[758])] = {{9559, 0}};
		mapped_strings[string_t(owned_strings[759])] = {{9556, 0}};
		mapped_strings[string_t(owned_strings[760])] = {{9558, 0}};
		mapped_strings[string_t(owned_strings[761])] = {{9555, 0}};
		mapped_strings[string_t(owned_strings[762])] = {{9552, 0}};
		mapped_strings[string_t(owned_strings[763])] = {{9574, 0}};
		mapped_strings[string_t(owned_strings[764])] = {{9577, 0}};
		mapped_strings[string_t(owned_strings[765])] = {{9572, 0}};
		mapped_strings[string_t(owned_strings[766])] = {{9575, 0}};
		mapped_strings[string_t(owned_strings[767])] = {{9565, 0}};
		mapped_strings[string_t(owned_strings[768])] = {{9562, 0}};
		mapped_strings[string_t(owned_strings[769])] = {{9564, 0}};
		mapped_strings[string_t(owned_strings[770])] = {{9561, 0}};
		mapped_strings[string_t(owned_strings[771])] = {{9553, 0}};
		mapped_strings[string_t(owned_strings[772])] = {{9580, 0}};
		mapped_strings[string_t(owned_strings[773])] = {{9571, 0}};
		mapped_strings[string_t(owned_strings[774])] = {{9568, 0}};
		mapped_strings[string_t(owned_strings[775])] = {{9579, 0}};
		mapped_strings[string_t(owned_strings[776])] = {{9570, 0}};
		mapped_strings[string_t(owned_strings[777])] = {{9567, 0}};
		mapped_strings[string_t(owned_strings[778])] = {{10697, 0}};
		mapped_strings[string_t(owned_strings[779])] = {{9557, 0}};
		mapped_strings[string_t(owned_strings[780])] = {{9554, 0}};
		mapped_strings[string_t(owned_strings[781])] = {{9488, 0}};
		mapped_strings[string_t(owned_strings[782])] = {{9484, 0}};
		mapped_strings[string_t(owned_strings[783])] = {{9472, 0}};
		mapped_strings[string_t(owned_strings[784])] = {{9573, 0}};
		mapped_strings[string_t(owned_strings[785])] = {{9576, 0}};
		mapped_strings[string_t(owned_strings[786])] = {{9516, 0}};
		mapped_strings[string_t(owned_strings[787])] = {{9524, 0}};
		mapped_strings[string_t(owned_strings[788])] = {{8863, 0}};
		mapped_strings[string_t(owned_strings[789])] = {{8862, 0}};
		mapped_strings[string_t(owned_strings[790])] = {{8864, 0}};
		mapped_strings[string_t(owned_strings[791])] = {{9563, 0}};
		mapped_strings[string_t(owned_strings[792])] = {{9560, 0}};
		mapped_strings[string_t(owned_strings[793])] = {{9496, 0}};
		mapped_strings[string_t(owned_strings[794])] = {{9492, 0}};
		mapped_strings[string_t(owned_strings[795])] = {{9474, 0}};
		mapped_strings[string_t(owned_strings[796])] = {{9578, 0}};
		mapped_strings[string_t(owned_strings[797])] = {{9569, 0}};
		mapped_strings[string_t(owned_strings[798])] = {{9566, 0}};
		mapped_strings[string_t(owned_strings[799])] = {{9532, 0}};
		mapped_strings[string_t(owned_strings[800])] = {{9508, 0}};
		mapped_strings[string_t(owned_strings[801])] = {{9500, 0}};
		mapped_strings[string_t(owned_strings[802])] = {{8245, 0}};
		mapped_strings[string_t(owned_strings[803])] = {{728, 0}};
		mapped_strings[string_t(owned_strings[804])] = {{166, 0}};
		mapped_strings[string_t(owned_strings[805])] = {{166, 0}};
		mapped_strings[string_t(owned_strings[806])] = {{119991, 0}};
		mapped_strings[string_t(owned_strings[807])] = {{8271, 0}};
		mapped_strings[string_t(owned_strings[808])] = {{8765, 0}};
		mapped_strings[string_t(owned_strings[809])] = {{8909, 0}};
		mapped_strings[string_t(owned_strings[810])] = {{92, 0}};
		mapped_strings[string_t(owned_strings[811])] = {{10693, 0}};
		mapped_strings[string_t(owned_strings[812])] = {{10184, 0}};
		mapped_strings[string_t(owned_strings[813])] = {{8226, 0}};
		mapped_strings[string_t(owned_strings[814])] = {{8226, 0}};
		mapped_strings[string_t(owned_strings[815])] = {{8782, 0}};
		mapped_strings[string_t(owned_strings[816])] = {{10926, 0}};
		mapped_strings[string_t(owned_strings[817])] = {{8783, 0}};
		mapped_strings[string_t(owned_strings[818])] = {{8783, 0}};
		mapped_strings[string_t(owned_strings[819])] = {{263, 0}};
		mapped_strings[string_t(owned_strings[820])] = {{8745, 0}};
		mapped_strings[string_t(owned_strings[821])] = {{10820, 0}};
		mapped_strings[string_t(owned_strings[822])] = {{10825, 0}};
		mapped_strings[string_t(owned_strings[823])] = {{10827, 0}};
		mapped_strings[string_t(owned_strings[824])] = {{10823, 0}};
		mapped_strings[string_t(owned_strings[825])] = {{10816, 0}};
		mapped_strings[string_t(owned_strings[826])] = {{8745, 65024}};
		mapped_strings[string_t(owned_strings[827])] = {{8257, 0}};
		mapped_strings[string_t(owned_strings[828])] = {{711, 0}};
		mapped_strings[string_t(owned_strings[829])] = {{10829, 0}};
		mapped_strings[string_t(owned_strings[830])] = {{269, 0}};
		mapped_strings[string_t(owned_strings[831])] = {{231, 0}};
		mapped_strings[string_t(owned_strings[832])] = {{231, 0}};
		mapped_strings[string_t(owned_strings[833])] = {{265, 0}};
		mapped_strings[string_t(owned_strings[834])] = {{10828, 0}};
		mapped_strings[string_t(owned_strings[835])] = {{10832, 0}};
		mapped_strings[string_t(owned_strings[836])] = {{267, 0}};
		mapped_strings[string_t(owned_strings[837])] = {{184, 0}};
		mapped_strings[string_t(owned_strings[838])] = {{184, 0}};
		mapped_strings[string_t(owned_strings[839])] = {{10674, 0}};
		mapped_strings[string_t(owned_strings[840])] = {{162, 0}};
		mapped_strings[string_t(owned_strings[841])] = {{162, 0}};
		mapped_strings[string_t(owned_strings[842])] = {{183, 0}};
		mapped_strings[string_t(owned_strings[843])] = {{120096, 0}};
		mapped_strings[string_t(owned_strings[844])] = {{1095, 0}};
		mapped_strings[string_t(owned_strings[845])] = {{10003, 0}};
		mapped_strings[string_t(owned_strings[846])] = {{10003, 0}};
		mapped_strings[string_t(owned_strings[847])] = {{967, 0}};
		mapped_strings[string_t(owned_strings[848])] = {{9675, 0}};
		mapped_strings[string_t(owned_strings[849])] = {{10691, 0}};
		mapped_strings[string_t(owned_strings[850])] = {{710, 0}};
		mapped_strings[string_t(owned_strings[851])] = {{8791, 0}};
		mapped_strings[string_t(owned_strings[852])] = {{8634, 0}};
		mapped_strings[string_t(owned_strings[853])] = {{8635, 0}};
		mapped_strings[string_t(owned_strings[854])] = {{174, 0}};
		mapped_strings[string_t(owned_strings[855])] = {{9416, 0}};
		mapped_strings[string_t(owned_strings[856])] = {{8859, 0}};
		mapped_strings[string_t(owned_strings[857])] = {{8858, 0}};
		mapped_strings[string_t(owned_strings[858])] = {{8861, 0}};
		mapped_strings[string_t(owned_strings[859])] = {{8791, 0}};
		mapped_strings[string_t(owned_strings[860])] = {{10768, 0}};
		mapped_strings[string_t(owned_strings[861])] = {{10991, 0}};
		mapped_strings[string_t(owned_strings[862])] = {{10690, 0}};
		mapped_strings[string_t(owned_strings[863])] = {{9827, 0}};
		mapped_strings[string_t(owned_strings[864])] = {{9827, 0}};
		mapped_strings[string_t(owned_strings[865])] = {{58, 0}};
		mapped_strings[string_t(owned_strings[866])] = {{8788, 0}};
		mapped_strings[string_t(owned_strings[867])] = {{8788, 0}};
		mapped_strings[string_t(owned_strings[868])] = {{44, 0}};
		mapped_strings[string_t(owned_strings[869])] = {{64, 0}};
		mapped_strings[string_t(owned_strings[870])] = {{8705, 0}};
		mapped_strings[string_t(owned_strings[871])] = {{8728, 0}};
		mapped_strings[string_t(owned_strings[872])] = {{8705, 0}};
		mapped_strings[string_t(owned_strings[873])] = {{8450, 0}};
		mapped_strings[string_t(owned_strings[874])] = {{8773, 0}};
		mapped_strings[string_t(owned_strings[875])] = {{10861, 0}};
		mapped_strings[string_t(owned_strings[876])] = {{8750, 0}};
		mapped_strings[string_t(owned_strings[877])] = {{120148, 0}};
		mapped_strings[string_t(owned_strings[878])] = {{8720, 0}};
		mapped_strings[string_t(owned_strings[879])] = {{169, 0}};
		mapped_strings[string_t(owned_strings[880])] = {{169, 0}};
		mapped_strings[string_t(owned_strings[881])] = {{8471, 0}};
		mapped_strings[string_t(owned_strings[882])] = {{8629, 0}};
		mapped_strings[string_t(owned_strings[883])] = {{10007, 0}};
		mapped_strings[string_t(owned_strings[884])] = {{119992, 0}};
		mapped_strings[string_t(owned_strings[885])] = {{10959, 0}};
		mapped_strings[string_t(owned_strings[886])] = {{10961, 0}};
		mapped_strings[string_t(owned_strings[887])] = {{10960, 0}};
		mapped_strings[string_t(owned_strings[888])] = {{10962, 0}};
		mapped_strings[string_t(owned_strings[889])] = {{8943, 0}};
		mapped_strings[string_t(owned_strings[890])] = {{10552, 0}};
		mapped_strings[string_t(owned_strings[891])] = {{10549, 0}};
		mapped_strings[string_t(owned_strings[892])] = {{8926, 0}};
		mapped_strings[string_t(owned_strings[893])] = {{8927, 0}};
		mapped_strings[string_t(owned_strings[894])] = {{8630, 0}};
		mapped_strings[string_t(owned_strings[895])] = {{10557, 0}};
		mapped_strings[string_t(owned_strings[896])] = {{8746, 0}};
		mapped_strings[string_t(owned_strings[897])] = {{10824, 0}};
		mapped_strings[string_t(owned_strings[898])] = {{10822, 0}};
		mapped_strings[string_t(owned_strings[899])] = {{10826, 0}};
		mapped_strings[string_t(owned_strings[900])] = {{8845, 0}};
		mapped_strings[string_t(owned_strings[901])] = {{10821, 0}};
		mapped_strings[string_t(owned_strings[902])] = {{8746, 65024}};
		mapped_strings[string_t(owned_strings[903])] = {{8631, 0}};
		mapped_strings[string_t(owned_strings[904])] = {{10556, 0}};
		mapped_strings[string_t(owned_strings[905])] = {{8926, 0}};
		mapped_strings[string_t(owned_strings[906])] = {{8927, 0}};
		mapped_strings[string_t(owned_strings[907])] = {{8910, 0}};
		mapped_strings[string_t(owned_strings[908])] = {{8911, 0}};
		mapped_strings[string_t(owned_strings[909])] = {{164, 0}};
		mapped_strings[string_t(owned_strings[910])] = {{164, 0}};
		mapped_strings[string_t(owned_strings[911])] = {{8630, 0}};
		mapped_strings[string_t(owned_strings[912])] = {{8631, 0}};
		mapped_strings[string_t(owned_strings[913])] = {{8910, 0}};
		mapped_strings[string_t(owned_strings[914])] = {{8911, 0}};
		mapped_strings[string_t(owned_strings[915])] = {{8754, 0}};
		mapped_strings[string_t(owned_strings[916])] = {{8753, 0}};
		mapped_strings[string_t(owned_strings[917])] = {{9005, 0}};
		mapped_strings[string_t(owned_strings[918])] = {{8659, 0}};
		mapped_strings[string_t(owned_strings[919])] = {{10597, 0}};
		mapped_strings[string_t(owned_strings[920])] = {{8224, 0}};
		mapped_strings[string_t(owned_strings[921])] = {{8504, 0}};
		mapped_strings[string_t(owned_strings[922])] = {{8595, 0}};
		mapped_strings[string_t(owned_strings[923])] = {{8208, 0}};
		mapped_strings[string_t(owned_strings[924])] = {{8867, 0}};
		mapped_strings[string_t(owned_strings[925])] = {{10511, 0}};
		mapped_strings[string_t(owned_strings[926])] = {{733, 0}};
		mapped_strings[string_t(owned_strings[927])] = {{271, 0}};
		mapped_strings[string_t(owned_strings[928])] = {{1076, 0}};
		mapped_strings[string_t(owned_strings[929])] = {{8518, 0}};
		mapped_strings[string_t(owned_strings[930])] = {{8225, 0}};
		mapped_strings[string_t(owned_strings[931])] = {{8650, 0}};
		mapped_strings[string_t(owned_strings[932])] = {{10871, 0}};
		mapped_strings[string_t(owned_strings[933])] = {{176, 0}};
		mapped_strings[string_t(owned_strings[934])] = {{176, 0}};
		mapped_strings[string_t(owned_strings[935])] = {{948, 0}};
		mapped_strings[string_t(owned_strings[936])] = {{10673, 0}};
		mapped_strings[string_t(owned_strings[937])] = {{10623, 0}};
		mapped_strings[string_t(owned_strings[938])] = {{120097, 0}};
		mapped_strings[string_t(owned_strings[939])] = {{8643, 0}};
		mapped_strings[string_t(owned_strings[940])] = {{8642, 0}};
		mapped_strings[string_t(owned_strings[941])] = {{8900, 0}};
		mapped_strings[string_t(owned_strings[942])] = {{8900, 0}};
		mapped_strings[string_t(owned_strings[943])] = {{9830, 0}};
		mapped_strings[string_t(owned_strings[944])] = {{9830, 0}};
		mapped_strings[string_t(owned_strings[945])] = {{168, 0}};
		mapped_strings[string_t(owned_strings[946])] = {{989, 0}};
		mapped_strings[string_t(owned_strings[947])] = {{8946, 0}};
		mapped_strings[string_t(owned_strings[948])] = {{247, 0}};
		mapped_strings[string_t(owned_strings[949])] = {{247, 0}};
		mapped_strings[string_t(owned_strings[950])] = {{247, 0}};
		mapped_strings[string_t(owned_strings[951])] = {{8903, 0}};
		mapped_strings[string_t(owned_strings[952])] = {{8903, 0}};
		mapped_strings[string_t(owned_strings[953])] = {{1106, 0}};
		mapped_strings[string_t(owned_strings[954])] = {{8990, 0}};
		mapped_strings[string_t(owned_strings[955])] = {{8973, 0}};
		mapped_strings[string_t(owned_strings[956])] = {{36, 0}};
		mapped_strings[string_t(owned_strings[957])] = {{120149, 0}};
		mapped_strings[string_t(owned_strings[958])] = {{729, 0}};
		mapped_strings[string_t(owned_strings[959])] = {{8784, 0}};
		mapped_strings[string_t(owned_strings[960])] = {{8785, 0}};
		mapped_strings[string_t(owned_strings[961])] = {{8760, 0}};
		mapped_strings[string_t(owned_strings[962])] = {{8724, 0}};
		mapped_strings[string_t(owned_strings[963])] = {{8865, 0}};
		mapped_strings[string_t(owned_strings[964])] = {{8966, 0}};
		mapped_strings[string_t(owned_strings[965])] = {{8595, 0}};
		mapped_strings[string_t(owned_strings[966])] = {{8650, 0}};
		mapped_strings[string_t(owned_strings[967])] = {{8643, 0}};
		mapped_strings[string_t(owned_strings[968])] = {{8642, 0}};
		mapped_strings[string_t(owned_strings[969])] = {{10512, 0}};
		mapped_strings[string_t(owned_strings[970])] = {{8991, 0}};
		mapped_strings[string_t(owned_strings[971])] = {{8972, 0}};
		mapped_strings[string_t(owned_strings[972])] = {{119993, 0}};
		mapped_strings[string_t(owned_strings[973])] = {{1109, 0}};
		mapped_strings[string_t(owned_strings[974])] = {{10742, 0}};
		mapped_strings[string_t(owned_strings[975])] = {{273, 0}};
		mapped_strings[string_t(owned_strings[976])] = {{8945, 0}};
		mapped_strings[string_t(owned_strings[977])] = {{9663, 0}};
		mapped_strings[string_t(owned_strings[978])] = {{9662, 0}};
		mapped_strings[string_t(owned_strings[979])] = {{8693, 0}};
		mapped_strings[string_t(owned_strings[980])] = {{10607, 0}};
		mapped_strings[string_t(owned_strings[981])] = {{10662, 0}};
		mapped_strings[string_t(owned_strings[982])] = {{1119, 0}};
		mapped_strings[string_t(owned_strings[983])] = {{10239, 0}};
		mapped_strings[string_t(owned_strings[984])] = {{10871, 0}};
		mapped_strings[string_t(owned_strings[985])] = {{8785, 0}};
		mapped_strings[string_t(owned_strings[986])] = {{233, 0}};
		mapped_strings[string_t(owned_strings[987])] = {{233, 0}};
		mapped_strings[string_t(owned_strings[988])] = {{10862, 0}};
		mapped_strings[string_t(owned_strings[989])] = {{283, 0}};
		mapped_strings[string_t(owned_strings[990])] = {{8790, 0}};
		mapped_strings[string_t(owned_strings[991])] = {{234, 0}};
		mapped_strings[string_t(owned_strings[992])] = {{234, 0}};
		mapped_strings[string_t(owned_strings[993])] = {{8789, 0}};
		mapped_strings[string_t(owned_strings[994])] = {{1101, 0}};
		mapped_strings[string_t(owned_strings[995])] = {{279, 0}};
		mapped_strings[string_t(owned_strings[996])] = {{8519, 0}};
		mapped_strings[string_t(owned_strings[997])] = {{8786, 0}};
		mapped_strings[string_t(owned_strings[998])] = {{120098, 0}};
		mapped_strings[string_t(owned_strings[999])] = {{10906, 0}};
		mapped_strings[string_t(owned_strings[1000])] = {{232, 0}};
		mapped_strings[string_t(owned_strings[1001])] = {{232, 0}};
		mapped_strings[string_t(owned_strings[1002])] = {{10902, 0}};
		mapped_strings[string_t(owned_strings[1003])] = {{10904, 0}};
		mapped_strings[string_t(owned_strings[1004])] = {{10905, 0}};
		mapped_strings[string_t(owned_strings[1005])] = {{9191, 0}};
		mapped_strings[string_t(owned_strings[1006])] = {{8467, 0}};
		mapped_strings[string_t(owned_strings[1007])] = {{10901, 0}};
		mapped_strings[string_t(owned_strings[1008])] = {{10903, 0}};
		mapped_strings[string_t(owned_strings[1009])] = {{275, 0}};
		mapped_strings[string_t(owned_strings[1010])] = {{8709, 0}};
		mapped_strings[string_t(owned_strings[1011])] = {{8709, 0}};
		mapped_strings[string_t(owned_strings[1012])] = {{8709, 0}};
		mapped_strings[string_t(owned_strings[1013])] = {{8196, 0}};
		mapped_strings[string_t(owned_strings[1014])] = {{8197, 0}};
		mapped_strings[string_t(owned_strings[1015])] = {{8195, 0}};
		mapped_strings[string_t(owned_strings[1016])] = {{331, 0}};
		mapped_strings[string_t(owned_strings[1017])] = {{8194, 0}};
		mapped_strings[string_t(owned_strings[1018])] = {{281, 0}};
		mapped_strings[string_t(owned_strings[1019])] = {{120150, 0}};
		mapped_strings[string_t(owned_strings[1020])] = {{8917, 0}};
		mapped_strings[string_t(owned_strings[1021])] = {{10723, 0}};
		mapped_strings[string_t(owned_strings[1022])] = {{10865, 0}};
		mapped_strings[string_t(owned_strings[1023])] = {{949, 0}};
		mapped_strings[string_t(owned_strings[1024])] = {{949, 0}};
		mapped_strings[string_t(owned_strings[1025])] = {{1013, 0}};
		mapped_strings[string_t(owned_strings[1026])] = {{8790, 0}};
		mapped_strings[string_t(owned_strings[1027])] = {{8789, 0}};
		mapped_strings[string_t(owned_strings[1028])] = {{8770, 0}};
		mapped_strings[string_t(owned_strings[1029])] = {{10902, 0}};
		mapped_strings[string_t(owned_strings[1030])] = {{10901, 0}};
		mapped_strings[string_t(owned_strings[1031])] = {{61, 0}};
		mapped_strings[string_t(owned_strings[1032])] = {{8799, 0}};
		mapped_strings[string_t(owned_strings[1033])] = {{8801, 0}};
		mapped_strings[string_t(owned_strings[1034])] = {{10872, 0}};
		mapped_strings[string_t(owned_strings[1035])] = {{10725, 0}};
		mapped_strings[string_t(owned_strings[1036])] = {{8787, 0}};
		mapped_strings[string_t(owned_strings[1037])] = {{10609, 0}};
		mapped_strings[string_t(owned_strings[1038])] = {{8495, 0}};
		mapped_strings[string_t(owned_strings[1039])] = {{8784, 0}};
		mapped_strings[string_t(owned_strings[1040])] = {{8770, 0}};
		mapped_strings[string_t(owned_strings[1041])] = {{951, 0}};
		mapped_strings[string_t(owned_strings[1042])] = {{240, 0}};
		mapped_strings[string_t(owned_strings[1043])] = {{240, 0}};
		mapped_strings[string_t(owned_strings[1044])] = {{235, 0}};
		mapped_strings[string_t(owned_strings[1045])] = {{235, 0}};
		mapped_strings[string_t(owned_strings[1046])] = {{8364, 0}};
		mapped_strings[string_t(owned_strings[1047])] = {{33, 0}};
		mapped_strings[string_t(owned_strings[1048])] = {{8707, 0}};
		mapped_strings[string_t(owned_strings[1049])] = {{8496, 0}};
		mapped_strings[string_t(owned_strings[1050])] = {{8519, 0}};
		mapped_strings[string_t(owned_strings[1051])] = {{8786, 0}};
		mapped_strings[string_t(owned_strings[1052])] = {{1092, 0}};
		mapped_strings[string_t(owned_strings[1053])] = {{9792, 0}};
		mapped_strings[string_t(owned_strings[1054])] = {{64259, 0}};
		mapped_strings[string_t(owned_strings[1055])] = {{64256, 0}};
		mapped_strings[string_t(owned_strings[1056])] = {{64260, 0}};
		mapped_strings[string_t(owned_strings[1057])] = {{120099, 0}};
		mapped_strings[string_t(owned_strings[1058])] = {{64257, 0}};
		mapped_strings[string_t(owned_strings[1059])] = {{102, 106}};
		mapped_strings[string_t(owned_strings[1060])] = {{9837, 0}};
		mapped_strings[string_t(owned_strings[1061])] = {{64258, 0}};
		mapped_strings[string_t(owned_strings[1062])] = {{9649, 0}};
		mapped_strings[string_t(owned_strings[1063])] = {{402, 0}};
		mapped_strings[string_t(owned_strings[1064])] = {{120151, 0}};
		mapped_strings[string_t(owned_strings[1065])] = {{8704, 0}};
		mapped_strings[string_t(owned_strings[1066])] = {{8916, 0}};
		mapped_strings[string_t(owned_strings[1067])] = {{10969, 0}};
		mapped_strings[string_t(owned_strings[1068])] = {{10765, 0}};
		mapped_strings[string_t(owned_strings[1069])] = {{189, 0}};
		mapped_strings[string_t(owned_strings[1070])] = {{189, 0}};
		mapped_strings[string_t(owned_strings[1071])] = {{8531, 0}};
		mapped_strings[string_t(owned_strings[1072])] = {{188, 0}};
		mapped_strings[string_t(owned_strings[1073])] = {{188, 0}};
		mapped_strings[string_t(owned_strings[1074])] = {{8533, 0}};
		mapped_strings[string_t(owned_strings[1075])] = {{8537, 0}};
		mapped_strings[string_t(owned_strings[1076])] = {{8539, 0}};
		mapped_strings[string_t(owned_strings[1077])] = {{8532, 0}};
		mapped_strings[string_t(owned_strings[1078])] = {{8534, 0}};
		mapped_strings[string_t(owned_strings[1079])] = {{190, 0}};
		mapped_strings[string_t(owned_strings[1080])] = {{190, 0}};
		mapped_strings[string_t(owned_strings[1081])] = {{8535, 0}};
		mapped_strings[string_t(owned_strings[1082])] = {{8540, 0}};
		mapped_strings[string_t(owned_strings[1083])] = {{8536, 0}};
		mapped_strings[string_t(owned_strings[1084])] = {{8538, 0}};
		mapped_strings[string_t(owned_strings[1085])] = {{8541, 0}};
		mapped_strings[string_t(owned_strings[1086])] = {{8542, 0}};
		mapped_strings[string_t(owned_strings[1087])] = {{8260, 0}};
		mapped_strings[string_t(owned_strings[1088])] = {{8994, 0}};
		mapped_strings[string_t(owned_strings[1089])] = {{119995, 0}};
		mapped_strings[string_t(owned_strings[1090])] = {{8807, 0}};
		mapped_strings[string_t(owned_strings[1091])] = {{10892, 0}};
		mapped_strings[string_t(owned_strings[1092])] = {{501, 0}};
		mapped_strings[string_t(owned_strings[1093])] = {{947, 0}};
		mapped_strings[string_t(owned_strings[1094])] = {{989, 0}};
		mapped_strings[string_t(owned_strings[1095])] = {{10886, 0}};
		mapped_strings[string_t(owned_strings[1096])] = {{287, 0}};
		mapped_strings[string_t(owned_strings[1097])] = {{285, 0}};
		mapped_strings[string_t(owned_strings[1098])] = {{1075, 0}};
		mapped_strings[string_t(owned_strings[1099])] = {{289, 0}};
		mapped_strings[string_t(owned_strings[1100])] = {{8805, 0}};
		mapped_strings[string_t(owned_strings[1101])] = {{8923, 0}};
		mapped_strings[string_t(owned_strings[1102])] = {{8805, 0}};
		mapped_strings[string_t(owned_strings[1103])] = {{8807, 0}};
		mapped_strings[string_t(owned_strings[1104])] = {{10878, 0}};
		mapped_strings[string_t(owned_strings[1105])] = {{10878, 0}};
		mapped_strings[string_t(owned_strings[1106])] = {{10921, 0}};
		mapped_strings[string_t(owned_strings[1107])] = {{10880, 0}};
		mapped_strings[string_t(owned_strings[1108])] = {{10882, 0}};
		mapped_strings[string_t(owned_strings[1109])] = {{10884, 0}};
		mapped_strings[string_t(owned_strings[1110])] = {{8923, 65024}};
		mapped_strings[string_t(owned_strings[1111])] = {{10900, 0}};
		mapped_strings[string_t(owned_strings[1112])] = {{120100, 0}};
		mapped_strings[string_t(owned_strings[1113])] = {{8811, 0}};
		mapped_strings[string_t(owned_strings[1114])] = {{8921, 0}};
		mapped_strings[string_t(owned_strings[1115])] = {{8503, 0}};
		mapped_strings[string_t(owned_strings[1116])] = {{1107, 0}};
		mapped_strings[string_t(owned_strings[1117])] = {{8823, 0}};
		mapped_strings[string_t(owned_strings[1118])] = {{10898, 0}};
		mapped_strings[string_t(owned_strings[1119])] = {{10917, 0}};
		mapped_strings[string_t(owned_strings[1120])] = {{10916, 0}};
		mapped_strings[string_t(owned_strings[1121])] = {{8809, 0}};
		mapped_strings[string_t(owned_strings[1122])] = {{10890, 0}};
		mapped_strings[string_t(owned_strings[1123])] = {{10890, 0}};
		mapped_strings[string_t(owned_strings[1124])] = {{10888, 0}};
		mapped_strings[string_t(owned_strings[1125])] = {{10888, 0}};
		mapped_strings[string_t(owned_strings[1126])] = {{8809, 0}};
		mapped_strings[string_t(owned_strings[1127])] = {{8935, 0}};
		mapped_strings[string_t(owned_strings[1128])] = {{120152, 0}};
		mapped_strings[string_t(owned_strings[1129])] = {{96, 0}};
		mapped_strings[string_t(owned_strings[1130])] = {{8458, 0}};
		mapped_strings[string_t(owned_strings[1131])] = {{8819, 0}};
		mapped_strings[string_t(owned_strings[1132])] = {{10894, 0}};
		mapped_strings[string_t(owned_strings[1133])] = {{10896, 0}};
		mapped_strings[string_t(owned_strings[1134])] = {{62, 0}};
		mapped_strings[string_t(owned_strings[1135])] = {{62, 0}};
		mapped_strings[string_t(owned_strings[1136])] = {{10919, 0}};
		mapped_strings[string_t(owned_strings[1137])] = {{10874, 0}};
		mapped_strings[string_t(owned_strings[1138])] = {{8919, 0}};
		mapped_strings[string_t(owned_strings[1139])] = {{10645, 0}};
		mapped_strings[string_t(owned_strings[1140])] = {{10876, 0}};
		mapped_strings[string_t(owned_strings[1141])] = {{10886, 0}};
		mapped_strings[string_t(owned_strings[1142])] = {{10616, 0}};
		mapped_strings[string_t(owned_strings[1143])] = {{8919, 0}};
		mapped_strings[string_t(owned_strings[1144])] = {{8923, 0}};
		mapped_strings[string_t(owned_strings[1145])] = {{10892, 0}};
		mapped_strings[string_t(owned_strings[1146])] = {{8823, 0}};
		mapped_strings[string_t(owned_strings[1147])] = {{8819, 0}};
		mapped_strings[string_t(owned_strings[1148])] = {{8809, 65024}};
		mapped_strings[string_t(owned_strings[1149])] = {{8809, 65024}};
		mapped_strings[string_t(owned_strings[1150])] = {{8660, 0}};
		mapped_strings[string_t(owned_strings[1151])] = {{8202, 0}};
		mapped_strings[string_t(owned_strings[1152])] = {{189, 0}};
		mapped_strings[string_t(owned_strings[1153])] = {{8459, 0}};
		mapped_strings[string_t(owned_strings[1154])] = {{1098, 0}};
		mapped_strings[string_t(owned_strings[1155])] = {{8596, 0}};
		mapped_strings[string_t(owned_strings[1156])] = {{10568, 0}};
		mapped_strings[string_t(owned_strings[1157])] = {{8621, 0}};
		mapped_strings[string_t(owned_strings[1158])] = {{8463, 0}};
		mapped_strings[string_t(owned_strings[1159])] = {{293, 0}};
		mapped_strings[string_t(owned_strings[1160])] = {{9829, 0}};
		mapped_strings[string_t(owned_strings[1161])] = {{9829, 0}};
		mapped_strings[string_t(owned_strings[1162])] = {{8230, 0}};
		mapped_strings[string_t(owned_strings[1163])] = {{8889, 0}};
		mapped_strings[string_t(owned_strings[1164])] = {{120101, 0}};
		mapped_strings[string_t(owned_strings[1165])] = {{10533, 0}};
		mapped_strings[string_t(owned_strings[1166])] = {{10534, 0}};
		mapped_strings[string_t(owned_strings[1167])] = {{8703, 0}};
		mapped_strings[string_t(owned_strings[1168])] = {{8763, 0}};
		mapped_strings[string_t(owned_strings[1169])] = {{8617, 0}};
		mapped_strings[string_t(owned_strings[1170])] = {{8618, 0}};
		mapped_strings[string_t(owned_strings[1171])] = {{120153, 0}};
		mapped_strings[string_t(owned_strings[1172])] = {{8213, 0}};
		mapped_strings[string_t(owned_strings[1173])] = {{119997, 0}};
		mapped_strings[string_t(owned_strings[1174])] = {{8463, 0}};
		mapped_strings[string_t(owned_strings[1175])] = {{295, 0}};
		mapped_strings[string_t(owned_strings[1176])] = {{8259, 0}};
		mapped_strings[string_t(owned_strings[1177])] = {{8208, 0}};
		mapped_strings[string_t(owned_strings[1178])] = {{237, 0}};
		mapped_strings[string_t(owned_strings[1179])] = {{237, 0}};
		mapped_strings[string_t(owned_strings[1180])] = {{8291, 0}};
		mapped_strings[string_t(owned_strings[1181])] = {{238, 0}};
		mapped_strings[string_t(owned_strings[1182])] = {{238, 0}};
		mapped_strings[string_t(owned_strings[1183])] = {{1080, 0}};
		mapped_strings[string_t(owned_strings[1184])] = {{1077, 0}};
		mapped_strings[string_t(owned_strings[1185])] = {{161, 0}};
		mapped_strings[string_t(owned_strings[1186])] = {{161, 0}};
		mapped_strings[string_t(owned_strings[1187])] = {{8660, 0}};
		mapped_strings[string_t(owned_strings[1188])] = {{120102, 0}};
		mapped_strings[string_t(owned_strings[1189])] = {{236, 0}};
		mapped_strings[string_t(owned_strings[1190])] = {{236, 0}};
		mapped_strings[string_t(owned_strings[1191])] = {{8520, 0}};
		mapped_strings[string_t(owned_strings[1192])] = {{10764, 0}};
		mapped_strings[string_t(owned_strings[1193])] = {{8749, 0}};
		mapped_strings[string_t(owned_strings[1194])] = {{10716, 0}};
		mapped_strings[string_t(owned_strings[1195])] = {{8489, 0}};
		mapped_strings[string_t(owned_strings[1196])] = {{307, 0}};
		mapped_strings[string_t(owned_strings[1197])] = {{299, 0}};
		mapped_strings[string_t(owned_strings[1198])] = {{8465, 0}};
		mapped_strings[string_t(owned_strings[1199])] = {{8464, 0}};
		mapped_strings[string_t(owned_strings[1200])] = {{8465, 0}};
		mapped_strings[string_t(owned_strings[1201])] = {{305, 0}};
		mapped_strings[string_t(owned_strings[1202])] = {{8887, 0}};
		mapped_strings[string_t(owned_strings[1203])] = {{437, 0}};
		mapped_strings[string_t(owned_strings[1204])] = {{8712, 0}};
		mapped_strings[string_t(owned_strings[1205])] = {{8453, 0}};
		mapped_strings[string_t(owned_strings[1206])] = {{8734, 0}};
		mapped_strings[string_t(owned_strings[1207])] = {{10717, 0}};
		mapped_strings[string_t(owned_strings[1208])] = {{305, 0}};
		mapped_strings[string_t(owned_strings[1209])] = {{8747, 0}};
		mapped_strings[string_t(owned_strings[1210])] = {{8890, 0}};
		mapped_strings[string_t(owned_strings[1211])] = {{8484, 0}};
		mapped_strings[string_t(owned_strings[1212])] = {{8890, 0}};
		mapped_strings[string_t(owned_strings[1213])] = {{10775, 0}};
		mapped_strings[string_t(owned_strings[1214])] = {{10812, 0}};
		mapped_strings[string_t(owned_strings[1215])] = {{1105, 0}};
		mapped_strings[string_t(owned_strings[1216])] = {{303, 0}};
		mapped_strings[string_t(owned_strings[1217])] = {{120154, 0}};
		mapped_strings[string_t(owned_strings[1218])] = {{953, 0}};
		mapped_strings[string_t(owned_strings[1219])] = {{10812, 0}};
		mapped_strings[string_t(owned_strings[1220])] = {{191, 0}};
		mapped_strings[string_t(owned_strings[1221])] = {{191, 0}};
		mapped_strings[string_t(owned_strings[1222])] = {{119998, 0}};
		mapped_strings[string_t(owned_strings[1223])] = {{8712, 0}};
		mapped_strings[string_t(owned_strings[1224])] = {{8953, 0}};
		mapped_strings[string_t(owned_strings[1225])] = {{8949, 0}};
		mapped_strings[string_t(owned_strings[1226])] = {{8948, 0}};
		mapped_strings[string_t(owned_strings[1227])] = {{8947, 0}};
		mapped_strings[string_t(owned_strings[1228])] = {{8712, 0}};
		mapped_strings[string_t(owned_strings[1229])] = {{8290, 0}};
		mapped_strings[string_t(owned_strings[1230])] = {{297, 0}};
		mapped_strings[string_t(owned_strings[1231])] = {{1110, 0}};
		mapped_strings[string_t(owned_strings[1232])] = {{239, 0}};
		mapped_strings[string_t(owned_strings[1233])] = {{239, 0}};
		mapped_strings[string_t(owned_strings[1234])] = {{309, 0}};
		mapped_strings[string_t(owned_strings[1235])] = {{1081, 0}};
		mapped_strings[string_t(owned_strings[1236])] = {{120103, 0}};
		mapped_strings[string_t(owned_strings[1237])] = {{567, 0}};
		mapped_strings[string_t(owned_strings[1238])] = {{120155, 0}};
		mapped_strings[string_t(owned_strings[1239])] = {{119999, 0}};
		mapped_strings[string_t(owned_strings[1240])] = {{1112, 0}};
		mapped_strings[string_t(owned_strings[1241])] = {{1108, 0}};
		mapped_strings[string_t(owned_strings[1242])] = {{954, 0}};
		mapped_strings[string_t(owned_strings[1243])] = {{1008, 0}};
		mapped_strings[string_t(owned_strings[1244])] = {{311, 0}};
		mapped_strings[string_t(owned_strings[1245])] = {{1082, 0}};
		mapped_strings[string_t(owned_strings[1246])] = {{120104, 0}};
		mapped_strings[string_t(owned_strings[1247])] = {{312, 0}};
		mapped_strings[string_t(owned_strings[1248])] = {{1093, 0}};
		mapped_strings[string_t(owned_strings[1249])] = {{1116, 0}};
		mapped_strings[string_t(owned_strings[1250])] = {{120156, 0}};
		mapped_strings[string_t(owned_strings[1251])] = {{120000, 0}};
		mapped_strings[string_t(owned_strings[1252])] = {{8666, 0}};
		mapped_strings[string_t(owned_strings[1253])] = {{8656, 0}};
		mapped_strings[string_t(owned_strings[1254])] = {{10523, 0}};
		mapped_strings[string_t(owned_strings[1255])] = {{10510, 0}};
		mapped_strings[string_t(owned_strings[1256])] = {{8806, 0}};
		mapped_strings[string_t(owned_strings[1257])] = {{10891, 0}};
		mapped_strings[string_t(owned_strings[1258])] = {{10594, 0}};
		mapped_strings[string_t(owned_strings[1259])] = {{314, 0}};
		mapped_strings[string_t(owned_strings[1260])] = {{10676, 0}};
		mapped_strings[string_t(owned_strings[1261])] = {{8466, 0}};
		mapped_strings[string_t(owned_strings[1262])] = {{955, 0}};
		mapped_strings[string_t(owned_strings[1263])] = {{10216, 0}};
		mapped_strings[string_t(owned_strings[1264])] = {{10641, 0}};
		mapped_strings[string_t(owned_strings[1265])] = {{10216, 0}};
		mapped_strings[string_t(owned_strings[1266])] = {{10885, 0}};
		mapped_strings[string_t(owned_strings[1267])] = {{171, 0}};
		mapped_strings[string_t(owned_strings[1268])] = {{171, 0}};
		mapped_strings[string_t(owned_strings[1269])] = {{8592, 0}};
		mapped_strings[string_t(owned_strings[1270])] = {{8676, 0}};
		mapped_strings[string_t(owned_strings[1271])] = {{10527, 0}};
		mapped_strings[string_t(owned_strings[1272])] = {{10525, 0}};
		mapped_strings[string_t(owned_strings[1273])] = {{8617, 0}};
		mapped_strings[string_t(owned_strings[1274])] = {{8619, 0}};
		mapped_strings[string_t(owned_strings[1275])] = {{10553, 0}};
		mapped_strings[string_t(owned_strings[1276])] = {{10611, 0}};
		mapped_strings[string_t(owned_strings[1277])] = {{8610, 0}};
		mapped_strings[string_t(owned_strings[1278])] = {{10923, 0}};
		mapped_strings[string_t(owned_strings[1279])] = {{10521, 0}};
		mapped_strings[string_t(owned_strings[1280])] = {{10925, 0}};
		mapped_strings[string_t(owned_strings[1281])] = {{10925, 65024}};
		mapped_strings[string_t(owned_strings[1282])] = {{10508, 0}};
		mapped_strings[string_t(owned_strings[1283])] = {{10098, 0}};
		mapped_strings[string_t(owned_strings[1284])] = {{123, 0}};
		mapped_strings[string_t(owned_strings[1285])] = {{91, 0}};
		mapped_strings[string_t(owned_strings[1286])] = {{10635, 0}};
		mapped_strings[string_t(owned_strings[1287])] = {{10639, 0}};
		mapped_strings[string_t(owned_strings[1288])] = {{10637, 0}};
		mapped_strings[string_t(owned_strings[1289])] = {{318, 0}};
		mapped_strings[string_t(owned_strings[1290])] = {{316, 0}};
		mapped_strings[string_t(owned_strings[1291])] = {{8968, 0}};
		mapped_strings[string_t(owned_strings[1292])] = {{123, 0}};
		mapped_strings[string_t(owned_strings[1293])] = {{1083, 0}};
		mapped_strings[string_t(owned_strings[1294])] = {{10550, 0}};
		mapped_strings[string_t(owned_strings[1295])] = {{8220, 0}};
		mapped_strings[string_t(owned_strings[1296])] = {{8222, 0}};
		mapped_strings[string_t(owned_strings[1297])] = {{10599, 0}};
		mapped_strings[string_t(owned_strings[1298])] = {{10571, 0}};
		mapped_strings[string_t(owned_strings[1299])] = {{8626, 0}};
		mapped_strings[string_t(owned_strings[1300])] = {{8804, 0}};
		mapped_strings[string_t(owned_strings[1301])] = {{8592, 0}};
		mapped_strings[string_t(owned_strings[1302])] = {{8610, 0}};
		mapped_strings[string_t(owned_strings[1303])] = {{8637, 0}};
		mapped_strings[string_t(owned_strings[1304])] = {{8636, 0}};
		mapped_strings[string_t(owned_strings[1305])] = {{8647, 0}};
		mapped_strings[string_t(owned_strings[1306])] = {{8596, 0}};
		mapped_strings[string_t(owned_strings[1307])] = {{8646, 0}};
		mapped_strings[string_t(owned_strings[1308])] = {{8651, 0}};
		mapped_strings[string_t(owned_strings[1309])] = {{8621, 0}};
		mapped_strings[string_t(owned_strings[1310])] = {{8907, 0}};
		mapped_strings[string_t(owned_strings[1311])] = {{8922, 0}};
		mapped_strings[string_t(owned_strings[1312])] = {{8804, 0}};
		mapped_strings[string_t(owned_strings[1313])] = {{8806, 0}};
		mapped_strings[string_t(owned_strings[1314])] = {{10877, 0}};
		mapped_strings[string_t(owned_strings[1315])] = {{10877, 0}};
		mapped_strings[string_t(owned_strings[1316])] = {{10920, 0}};
		mapped_strings[string_t(owned_strings[1317])] = {{10879, 0}};
		mapped_strings[string_t(owned_strings[1318])] = {{10881, 0}};
		mapped_strings[string_t(owned_strings[1319])] = {{10883, 0}};
		mapped_strings[string_t(owned_strings[1320])] = {{8922, 65024}};
		mapped_strings[string_t(owned_strings[1321])] = {{10899, 0}};
		mapped_strings[string_t(owned_strings[1322])] = {{10885, 0}};
		mapped_strings[string_t(owned_strings[1323])] = {{8918, 0}};
		mapped_strings[string_t(owned_strings[1324])] = {{8922, 0}};
		mapped_strings[string_t(owned_strings[1325])] = {{10891, 0}};
		mapped_strings[string_t(owned_strings[1326])] = {{8822, 0}};
		mapped_strings[string_t(owned_strings[1327])] = {{8818, 0}};
		mapped_strings[string_t(owned_strings[1328])] = {{10620, 0}};
		mapped_strings[string_t(owned_strings[1329])] = {{8970, 0}};
		mapped_strings[string_t(owned_strings[1330])] = {{120105, 0}};
		mapped_strings[string_t(owned_strings[1331])] = {{8822, 0}};
		mapped_strings[string_t(owned_strings[1332])] = {{10897, 0}};
		mapped_strings[string_t(owned_strings[1333])] = {{8637, 0}};
		mapped_strings[string_t(owned_strings[1334])] = {{8636, 0}};
		mapped_strings[string_t(owned_strings[1335])] = {{10602, 0}};
		mapped_strings[string_t(owned_strings[1336])] = {{9604, 0}};
		mapped_strings[string_t(owned_strings[1337])] = {{1113, 0}};
		mapped_strings[string_t(owned_strings[1338])] = {{8810, 0}};
		mapped_strings[string_t(owned_strings[1339])] = {{8647, 0}};
		mapped_strings[string_t(owned_strings[1340])] = {{8990, 0}};
		mapped_strings[string_t(owned_strings[1341])] = {{10603, 0}};
		mapped_strings[string_t(owned_strings[1342])] = {{9722, 0}};
		mapped_strings[string_t(owned_strings[1343])] = {{320, 0}};
		mapped_strings[string_t(owned_strings[1344])] = {{9136, 0}};
		mapped_strings[string_t(owned_strings[1345])] = {{9136, 0}};
		mapped_strings[string_t(owned_strings[1346])] = {{8808, 0}};
		mapped_strings[string_t(owned_strings[1347])] = {{10889, 0}};
		mapped_strings[string_t(owned_strings[1348])] = {{10889, 0}};
		mapped_strings[string_t(owned_strings[1349])] = {{10887, 0}};
		mapped_strings[string_t(owned_strings[1350])] = {{10887, 0}};
		mapped_strings[string_t(owned_strings[1351])] = {{8808, 0}};
		mapped_strings[string_t(owned_strings[1352])] = {{8934, 0}};
		mapped_strings[string_t(owned_strings[1353])] = {{10220, 0}};
		mapped_strings[string_t(owned_strings[1354])] = {{8701, 0}};
		mapped_strings[string_t(owned_strings[1355])] = {{10214, 0}};
		mapped_strings[string_t(owned_strings[1356])] = {{10229, 0}};
		mapped_strings[string_t(owned_strings[1357])] = {{10231, 0}};
		mapped_strings[string_t(owned_strings[1358])] = {{10236, 0}};
		mapped_strings[string_t(owned_strings[1359])] = {{10230, 0}};
		mapped_strings[string_t(owned_strings[1360])] = {{8619, 0}};
		mapped_strings[string_t(owned_strings[1361])] = {{8620, 0}};
		mapped_strings[string_t(owned_strings[1362])] = {{10629, 0}};
		mapped_strings[string_t(owned_strings[1363])] = {{120157, 0}};
		mapped_strings[string_t(owned_strings[1364])] = {{10797, 0}};
		mapped_strings[string_t(owned_strings[1365])] = {{10804, 0}};
		mapped_strings[string_t(owned_strings[1366])] = {{8727, 0}};
		mapped_strings[string_t(owned_strings[1367])] = {{95, 0}};
		mapped_strings[string_t(owned_strings[1368])] = {{9674, 0}};
		mapped_strings[string_t(owned_strings[1369])] = {{9674, 0}};
		mapped_strings[string_t(owned_strings[1370])] = {{10731, 0}};
		mapped_strings[string_t(owned_strings[1371])] = {{40, 0}};
		mapped_strings[string_t(owned_strings[1372])] = {{10643, 0}};
		mapped_strings[string_t(owned_strings[1373])] = {{8646, 0}};
		mapped_strings[string_t(owned_strings[1374])] = {{8991, 0}};
		mapped_strings[string_t(owned_strings[1375])] = {{8651, 0}};
		mapped_strings[string_t(owned_strings[1376])] = {{10605, 0}};
		mapped_strings[string_t(owned_strings[1377])] = {{8206, 0}};
		mapped_strings[string_t(owned_strings[1378])] = {{8895, 0}};
		mapped_strings[string_t(owned_strings[1379])] = {{8249, 0}};
		mapped_strings[string_t(owned_strings[1380])] = {{120001, 0}};
		mapped_strings[string_t(owned_strings[1381])] = {{8624, 0}};
		mapped_strings[string_t(owned_strings[1382])] = {{8818, 0}};
		mapped_strings[string_t(owned_strings[1383])] = {{10893, 0}};
		mapped_strings[string_t(owned_strings[1384])] = {{10895, 0}};
		mapped_strings[string_t(owned_strings[1385])] = {{91, 0}};
		mapped_strings[string_t(owned_strings[1386])] = {{8216, 0}};
		mapped_strings[string_t(owned_strings[1387])] = {{8218, 0}};
		mapped_strings[string_t(owned_strings[1388])] = {{322, 0}};
		mapped_strings[string_t(owned_strings[1389])] = {{60, 0}};
		mapped_strings[string_t(owned_strings[1390])] = {{60, 0}};
		mapped_strings[string_t(owned_strings[1391])] = {{10918, 0}};
		mapped_strings[string_t(owned_strings[1392])] = {{10873, 0}};
		mapped_strings[string_t(owned_strings[1393])] = {{8918, 0}};
		mapped_strings[string_t(owned_strings[1394])] = {{8907, 0}};
		mapped_strings[string_t(owned_strings[1395])] = {{8905, 0}};
		mapped_strings[string_t(owned_strings[1396])] = {{10614, 0}};
		mapped_strings[string_t(owned_strings[1397])] = {{10875, 0}};
		mapped_strings[string_t(owned_strings[1398])] = {{10646, 0}};
		mapped_strings[string_t(owned_strings[1399])] = {{9667, 0}};
		mapped_strings[string_t(owned_strings[1400])] = {{8884, 0}};
		mapped_strings[string_t(owned_strings[1401])] = {{9666, 0}};
		mapped_strings[string_t(owned_strings[1402])] = {{10570, 0}};
		mapped_strings[string_t(owned_strings[1403])] = {{10598, 0}};
		mapped_strings[string_t(owned_strings[1404])] = {{8808, 65024}};
		mapped_strings[string_t(owned_strings[1405])] = {{8808, 65024}};
		mapped_strings[string_t(owned_strings[1406])] = {{8762, 0}};
		mapped_strings[string_t(owned_strings[1407])] = {{175, 0}};
		mapped_strings[string_t(owned_strings[1408])] = {{175, 0}};
		mapped_strings[string_t(owned_strings[1409])] = {{9794, 0}};
		mapped_strings[string_t(owned_strings[1410])] = {{10016, 0}};
		mapped_strings[string_t(owned_strings[1411])] = {{10016, 0}};
		mapped_strings[string_t(owned_strings[1412])] = {{8614, 0}};
		mapped_strings[string_t(owned_strings[1413])] = {{8614, 0}};
		mapped_strings[string_t(owned_strings[1414])] = {{8615, 0}};
		mapped_strings[string_t(owned_strings[1415])] = {{8612, 0}};
		mapped_strings[string_t(owned_strings[1416])] = {{8613, 0}};
		mapped_strings[string_t(owned_strings[1417])] = {{9646, 0}};
		mapped_strings[string_t(owned_strings[1418])] = {{10793, 0}};
		mapped_strings[string_t(owned_strings[1419])] = {{1084, 0}};
		mapped_strings[string_t(owned_strings[1420])] = {{8212, 0}};
		mapped_strings[string_t(owned_strings[1421])] = {{8737, 0}};
		mapped_strings[string_t(owned_strings[1422])] = {{120106, 0}};
		mapped_strings[string_t(owned_strings[1423])] = {{8487, 0}};
		mapped_strings[string_t(owned_strings[1424])] = {{181, 0}};
		mapped_strings[string_t(owned_strings[1425])] = {{181, 0}};
		mapped_strings[string_t(owned_strings[1426])] = {{8739, 0}};
		mapped_strings[string_t(owned_strings[1427])] = {{42, 0}};
		mapped_strings[string_t(owned_strings[1428])] = {{10992, 0}};
		mapped_strings[string_t(owned_strings[1429])] = {{183, 0}};
		mapped_strings[string_t(owned_strings[1430])] = {{183, 0}};
		mapped_strings[string_t(owned_strings[1431])] = {{8722, 0}};
		mapped_strings[string_t(owned_strings[1432])] = {{8863, 0}};
		mapped_strings[string_t(owned_strings[1433])] = {{8760, 0}};
		mapped_strings[string_t(owned_strings[1434])] = {{10794, 0}};
		mapped_strings[string_t(owned_strings[1435])] = {{10971, 0}};
		mapped_strings[string_t(owned_strings[1436])] = {{8230, 0}};
		mapped_strings[string_t(owned_strings[1437])] = {{8723, 0}};
		mapped_strings[string_t(owned_strings[1438])] = {{8871, 0}};
		mapped_strings[string_t(owned_strings[1439])] = {{120158, 0}};
		mapped_strings[string_t(owned_strings[1440])] = {{8723, 0}};
		mapped_strings[string_t(owned_strings[1441])] = {{120002, 0}};
		mapped_strings[string_t(owned_strings[1442])] = {{8766, 0}};
		mapped_strings[string_t(owned_strings[1443])] = {{956, 0}};
		mapped_strings[string_t(owned_strings[1444])] = {{8888, 0}};
		mapped_strings[string_t(owned_strings[1445])] = {{8888, 0}};
		mapped_strings[string_t(owned_strings[1446])] = {{8921, 824}};
		mapped_strings[string_t(owned_strings[1447])] = {{8811, 8402}};
		mapped_strings[string_t(owned_strings[1448])] = {{8811, 824}};
		mapped_strings[string_t(owned_strings[1449])] = {{8653, 0}};
		mapped_strings[string_t(owned_strings[1450])] = {{8654, 0}};
		mapped_strings[string_t(owned_strings[1451])] = {{8920, 824}};
		mapped_strings[string_t(owned_strings[1452])] = {{8810, 8402}};
		mapped_strings[string_t(owned_strings[1453])] = {{8810, 824}};
		mapped_strings[string_t(owned_strings[1454])] = {{8655, 0}};
		mapped_strings[string_t(owned_strings[1455])] = {{8879, 0}};
		mapped_strings[string_t(owned_strings[1456])] = {{8878, 0}};
		mapped_strings[string_t(owned_strings[1457])] = {{8711, 0}};
		mapped_strings[string_t(owned_strings[1458])] = {{324, 0}};
		mapped_strings[string_t(owned_strings[1459])] = {{8736, 8402}};
		mapped_strings[string_t(owned_strings[1460])] = {{8777, 0}};
		mapped_strings[string_t(owned_strings[1461])] = {{10864, 824}};
		mapped_strings[string_t(owned_strings[1462])] = {{8779, 824}};
		mapped_strings[string_t(owned_strings[1463])] = {{329, 0}};
		mapped_strings[string_t(owned_strings[1464])] = {{8777, 0}};
		mapped_strings[string_t(owned_strings[1465])] = {{9838, 0}};
		mapped_strings[string_t(owned_strings[1466])] = {{9838, 0}};
		mapped_strings[string_t(owned_strings[1467])] = {{8469, 0}};
		mapped_strings[string_t(owned_strings[1468])] = {{160, 0}};
		mapped_strings[string_t(owned_strings[1469])] = {{160, 0}};
		mapped_strings[string_t(owned_strings[1470])] = {{8782, 824}};
		mapped_strings[string_t(owned_strings[1471])] = {{8783, 824}};
		mapped_strings[string_t(owned_strings[1472])] = {{10819, 0}};
		mapped_strings[string_t(owned_strings[1473])] = {{328, 0}};
		mapped_strings[string_t(owned_strings[1474])] = {{326, 0}};
		mapped_strings[string_t(owned_strings[1475])] = {{8775, 0}};
		mapped_strings[string_t(owned_strings[1476])] = {{10861, 824}};
		mapped_strings[string_t(owned_strings[1477])] = {{10818, 0}};
		mapped_strings[string_t(owned_strings[1478])] = {{1085, 0}};
		mapped_strings[string_t(owned_strings[1479])] = {{8211, 0}};
		mapped_strings[string_t(owned_strings[1480])] = {{8800, 0}};
		mapped_strings[string_t(owned_strings[1481])] = {{8663, 0}};
		mapped_strings[string_t(owned_strings[1482])] = {{10532, 0}};
		mapped_strings[string_t(owned_strings[1483])] = {{8599, 0}};
		mapped_strings[string_t(owned_strings[1484])] = {{8599, 0}};
		mapped_strings[string_t(owned_strings[1485])] = {{8784, 824}};
		mapped_strings[string_t(owned_strings[1486])] = {{8802, 0}};
		mapped_strings[string_t(owned_strings[1487])] = {{10536, 0}};
		mapped_strings[string_t(owned_strings[1488])] = {{8770, 824}};
		mapped_strings[string_t(owned_strings[1489])] = {{8708, 0}};
		mapped_strings[string_t(owned_strings[1490])] = {{8708, 0}};
		mapped_strings[string_t(owned_strings[1491])] = {{120107, 0}};
		mapped_strings[string_t(owned_strings[1492])] = {{8807, 824}};
		mapped_strings[string_t(owned_strings[1493])] = {{8817, 0}};
		mapped_strings[string_t(owned_strings[1494])] = {{8817, 0}};
		mapped_strings[string_t(owned_strings[1495])] = {{8807, 824}};
		mapped_strings[string_t(owned_strings[1496])] = {{10878, 824}};
		mapped_strings[string_t(owned_strings[1497])] = {{10878, 824}};
		mapped_strings[string_t(owned_strings[1498])] = {{8821, 0}};
		mapped_strings[string_t(owned_strings[1499])] = {{8815, 0}};
		mapped_strings[string_t(owned_strings[1500])] = {{8815, 0}};
		mapped_strings[string_t(owned_strings[1501])] = {{8654, 0}};
		mapped_strings[string_t(owned_strings[1502])] = {{8622, 0}};
		mapped_strings[string_t(owned_strings[1503])] = {{10994, 0}};
		mapped_strings[string_t(owned_strings[1504])] = {{8715, 0}};
		mapped_strings[string_t(owned_strings[1505])] = {{8956, 0}};
		mapped_strings[string_t(owned_strings[1506])] = {{8954, 0}};
		mapped_strings[string_t(owned_strings[1507])] = {{8715, 0}};
		mapped_strings[string_t(owned_strings[1508])] = {{1114, 0}};
		mapped_strings[string_t(owned_strings[1509])] = {{8653, 0}};
		mapped_strings[string_t(owned_strings[1510])] = {{8806, 824}};
		mapped_strings[string_t(owned_strings[1511])] = {{8602, 0}};
		mapped_strings[string_t(owned_strings[1512])] = {{8229, 0}};
		mapped_strings[string_t(owned_strings[1513])] = {{8816, 0}};
		mapped_strings[string_t(owned_strings[1514])] = {{8602, 0}};
		mapped_strings[string_t(owned_strings[1515])] = {{8622, 0}};
		mapped_strings[string_t(owned_strings[1516])] = {{8816, 0}};
		mapped_strings[string_t(owned_strings[1517])] = {{8806, 824}};
		mapped_strings[string_t(owned_strings[1518])] = {{10877, 824}};
		mapped_strings[string_t(owned_strings[1519])] = {{10877, 824}};
		mapped_strings[string_t(owned_strings[1520])] = {{8814, 0}};
		mapped_strings[string_t(owned_strings[1521])] = {{8820, 0}};
		mapped_strings[string_t(owned_strings[1522])] = {{8814, 0}};
		mapped_strings[string_t(owned_strings[1523])] = {{8938, 0}};
		mapped_strings[string_t(owned_strings[1524])] = {{8940, 0}};
		mapped_strings[string_t(owned_strings[1525])] = {{8740, 0}};
		mapped_strings[string_t(owned_strings[1526])] = {{120159, 0}};
		mapped_strings[string_t(owned_strings[1527])] = {{172, 0}};
		mapped_strings[string_t(owned_strings[1528])] = {{172, 0}};
		mapped_strings[string_t(owned_strings[1529])] = {{8713, 0}};
		mapped_strings[string_t(owned_strings[1530])] = {{8953, 824}};
		mapped_strings[string_t(owned_strings[1531])] = {{8949, 824}};
		mapped_strings[string_t(owned_strings[1532])] = {{8713, 0}};
		mapped_strings[string_t(owned_strings[1533])] = {{8951, 0}};
		mapped_strings[string_t(owned_strings[1534])] = {{8950, 0}};
		mapped_strings[string_t(owned_strings[1535])] = {{8716, 0}};
		mapped_strings[string_t(owned_strings[1536])] = {{8716, 0}};
		mapped_strings[string_t(owned_strings[1537])] = {{8958, 0}};
		mapped_strings[string_t(owned_strings[1538])] = {{8957, 0}};
		mapped_strings[string_t(owned_strings[1539])] = {{8742, 0}};
		mapped_strings[string_t(owned_strings[1540])] = {{8742, 0}};
		mapped_strings[string_t(owned_strings[1541])] = {{11005, 8421}};
		mapped_strings[string_t(owned_strings[1542])] = {{8706, 824}};
		mapped_strings[string_t(owned_strings[1543])] = {{10772, 0}};
		mapped_strings[string_t(owned_strings[1544])] = {{8832, 0}};
		mapped_strings[string_t(owned_strings[1545])] = {{8928, 0}};
		mapped_strings[string_t(owned_strings[1546])] = {{10927, 824}};
		mapped_strings[string_t(owned_strings[1547])] = {{8832, 0}};
		mapped_strings[string_t(owned_strings[1548])] = {{10927, 824}};
		mapped_strings[string_t(owned_strings[1549])] = {{8655, 0}};
		mapped_strings[string_t(owned_strings[1550])] = {{8603, 0}};
		mapped_strings[string_t(owned_strings[1551])] = {{10547, 824}};
		mapped_strings[string_t(owned_strings[1552])] = {{8605, 824}};
		mapped_strings[string_t(owned_strings[1553])] = {{8603, 0}};
		mapped_strings[string_t(owned_strings[1554])] = {{8939, 0}};
		mapped_strings[string_t(owned_strings[1555])] = {{8941, 0}};
		mapped_strings[string_t(owned_strings[1556])] = {{8833, 0}};
		mapped_strings[string_t(owned_strings[1557])] = {{8929, 0}};
		mapped_strings[string_t(owned_strings[1558])] = {{10928, 824}};
		mapped_strings[string_t(owned_strings[1559])] = {{120003, 0}};
		mapped_strings[string_t(owned_strings[1560])] = {{8740, 0}};
		mapped_strings[string_t(owned_strings[1561])] = {{8742, 0}};
		mapped_strings[string_t(owned_strings[1562])] = {{8769, 0}};
		mapped_strings[string_t(owned_strings[1563])] = {{8772, 0}};
		mapped_strings[string_t(owned_strings[1564])] = {{8772, 0}};
		mapped_strings[string_t(owned_strings[1565])] = {{8740, 0}};
		mapped_strings[string_t(owned_strings[1566])] = {{8742, 0}};
		mapped_strings[string_t(owned_strings[1567])] = {{8930, 0}};
		mapped_strings[string_t(owned_strings[1568])] = {{8931, 0}};
		mapped_strings[string_t(owned_strings[1569])] = {{8836, 0}};
		mapped_strings[string_t(owned_strings[1570])] = {{10949, 824}};
		mapped_strings[string_t(owned_strings[1571])] = {{8840, 0}};
		mapped_strings[string_t(owned_strings[1572])] = {{8834, 8402}};
		mapped_strings[string_t(owned_strings[1573])] = {{8840, 0}};
		mapped_strings[string_t(owned_strings[1574])] = {{10949, 824}};
		mapped_strings[string_t(owned_strings[1575])] = {{8833, 0}};
		mapped_strings[string_t(owned_strings[1576])] = {{10928, 824}};
		mapped_strings[string_t(owned_strings[1577])] = {{8837, 0}};
		mapped_strings[string_t(owned_strings[1578])] = {{10950, 824}};
		mapped_strings[string_t(owned_strings[1579])] = {{8841, 0}};
		mapped_strings[string_t(owned_strings[1580])] = {{8835, 8402}};
		mapped_strings[string_t(owned_strings[1581])] = {{8841, 0}};
		mapped_strings[string_t(owned_strings[1582])] = {{10950, 824}};
		mapped_strings[string_t(owned_strings[1583])] = {{8825, 0}};
		mapped_strings[string_t(owned_strings[1584])] = {{241, 0}};
		mapped_strings[string_t(owned_strings[1585])] = {{241, 0}};
		mapped_strings[string_t(owned_strings[1586])] = {{8824, 0}};
		mapped_strings[string_t(owned_strings[1587])] = {{8938, 0}};
		mapped_strings[string_t(owned_strings[1588])] = {{8940, 0}};
		mapped_strings[string_t(owned_strings[1589])] = {{8939, 0}};
		mapped_strings[string_t(owned_strings[1590])] = {{8941, 0}};
		mapped_strings[string_t(owned_strings[1591])] = {{957, 0}};
		mapped_strings[string_t(owned_strings[1592])] = {{35, 0}};
		mapped_strings[string_t(owned_strings[1593])] = {{8470, 0}};
		mapped_strings[string_t(owned_strings[1594])] = {{8199, 0}};
		mapped_strings[string_t(owned_strings[1595])] = {{8877, 0}};
		mapped_strings[string_t(owned_strings[1596])] = {{10500, 0}};
		mapped_strings[string_t(owned_strings[1597])] = {{8781, 8402}};
		mapped_strings[string_t(owned_strings[1598])] = {{8876, 0}};
		mapped_strings[string_t(owned_strings[1599])] = {{8805, 8402}};
		mapped_strings[string_t(owned_strings[1600])] = {{62, 8402}};
		mapped_strings[string_t(owned_strings[1601])] = {{10718, 0}};
		mapped_strings[string_t(owned_strings[1602])] = {{10498, 0}};
		mapped_strings[string_t(owned_strings[1603])] = {{8804, 8402}};
		mapped_strings[string_t(owned_strings[1604])] = {{60, 8402}};
		mapped_strings[string_t(owned_strings[1605])] = {{8884, 8402}};
		mapped_strings[string_t(owned_strings[1606])] = {{10499, 0}};
		mapped_strings[string_t(owned_strings[1607])] = {{8885, 8402}};
		mapped_strings[string_t(owned_strings[1608])] = {{8764, 8402}};
		mapped_strings[string_t(owned_strings[1609])] = {{8662, 0}};
		mapped_strings[string_t(owned_strings[1610])] = {{10531, 0}};
		mapped_strings[string_t(owned_strings[1611])] = {{8598, 0}};
		mapped_strings[string_t(owned_strings[1612])] = {{8598, 0}};
		mapped_strings[string_t(owned_strings[1613])] = {{10535, 0}};
		mapped_strings[string_t(owned_strings[1614])] = {{9416, 0}};
		mapped_strings[string_t(owned_strings[1615])] = {{243, 0}};
		mapped_strings[string_t(owned_strings[1616])] = {{243, 0}};
		mapped_strings[string_t(owned_strings[1617])] = {{8859, 0}};
		mapped_strings[string_t(owned_strings[1618])] = {{8858, 0}};
		mapped_strings[string_t(owned_strings[1619])] = {{244, 0}};
		mapped_strings[string_t(owned_strings[1620])] = {{244, 0}};
		mapped_strings[string_t(owned_strings[1621])] = {{1086, 0}};
		mapped_strings[string_t(owned_strings[1622])] = {{8861, 0}};
		mapped_strings[string_t(owned_strings[1623])] = {{337, 0}};
		mapped_strings[string_t(owned_strings[1624])] = {{10808, 0}};
		mapped_strings[string_t(owned_strings[1625])] = {{8857, 0}};
		mapped_strings[string_t(owned_strings[1626])] = {{10684, 0}};
		mapped_strings[string_t(owned_strings[1627])] = {{339, 0}};
		mapped_strings[string_t(owned_strings[1628])] = {{10687, 0}};
		mapped_strings[string_t(owned_strings[1629])] = {{120108, 0}};
		mapped_strings[string_t(owned_strings[1630])] = {{731, 0}};
		mapped_strings[string_t(owned_strings[1631])] = {{242, 0}};
		mapped_strings[string_t(owned_strings[1632])] = {{242, 0}};
		mapped_strings[string_t(owned_strings[1633])] = {{10689, 0}};
		mapped_strings[string_t(owned_strings[1634])] = {{10677, 0}};
		mapped_strings[string_t(owned_strings[1635])] = {{937, 0}};
		mapped_strings[string_t(owned_strings[1636])] = {{8750, 0}};
		mapped_strings[string_t(owned_strings[1637])] = {{8634, 0}};
		mapped_strings[string_t(owned_strings[1638])] = {{10686, 0}};
		mapped_strings[string_t(owned_strings[1639])] = {{10683, 0}};
		mapped_strings[string_t(owned_strings[1640])] = {{8254, 0}};
		mapped_strings[string_t(owned_strings[1641])] = {{10688, 0}};
		mapped_strings[string_t(owned_strings[1642])] = {{333, 0}};
		mapped_strings[string_t(owned_strings[1643])] = {{969, 0}};
		mapped_strings[string_t(owned_strings[1644])] = {{959, 0}};
		mapped_strings[string_t(owned_strings[1645])] = {{10678, 0}};
		mapped_strings[string_t(owned_strings[1646])] = {{8854, 0}};
		mapped_strings[string_t(owned_strings[1647])] = {{120160, 0}};
		mapped_strings[string_t(owned_strings[1648])] = {{10679, 0}};
		mapped_strings[string_t(owned_strings[1649])] = {{10681, 0}};
		mapped_strings[string_t(owned_strings[1650])] = {{8853, 0}};
		mapped_strings[string_t(owned_strings[1651])] = {{8744, 0}};
		mapped_strings[string_t(owned_strings[1652])] = {{8635, 0}};
		mapped_strings[string_t(owned_strings[1653])] = {{10845, 0}};
		mapped_strings[string_t(owned_strings[1654])] = {{8500, 0}};
		mapped_strings[string_t(owned_strings[1655])] = {{8500, 0}};
		mapped_strings[string_t(owned_strings[1656])] = {{170, 0}};
		mapped_strings[string_t(owned_strings[1657])] = {{170, 0}};
		mapped_strings[string_t(owned_strings[1658])] = {{186, 0}};
		mapped_strings[string_t(owned_strings[1659])] = {{186, 0}};
		mapped_strings[string_t(owned_strings[1660])] = {{8886, 0}};
		mapped_strings[string_t(owned_strings[1661])] = {{10838, 0}};
		mapped_strings[string_t(owned_strings[1662])] = {{10839, 0}};
		mapped_strings[string_t(owned_strings[1663])] = {{10843, 0}};
		mapped_strings[string_t(owned_strings[1664])] = {{8500, 0}};
		mapped_strings[string_t(owned_strings[1665])] = {{248, 0}};
		mapped_strings[string_t(owned_strings[1666])] = {{248, 0}};
		mapped_strings[string_t(owned_strings[1667])] = {{8856, 0}};
		mapped_strings[string_t(owned_strings[1668])] = {{245, 0}};
		mapped_strings[string_t(owned_strings[1669])] = {{245, 0}};
		mapped_strings[string_t(owned_strings[1670])] = {{8855, 0}};
		mapped_strings[string_t(owned_strings[1671])] = {{10806, 0}};
		mapped_strings[string_t(owned_strings[1672])] = {{246, 0}};
		mapped_strings[string_t(owned_strings[1673])] = {{246, 0}};
		mapped_strings[string_t(owned_strings[1674])] = {{9021, 0}};
		mapped_strings[string_t(owned_strings[1675])] = {{8741, 0}};
		mapped_strings[string_t(owned_strings[1676])] = {{182, 0}};
		mapped_strings[string_t(owned_strings[1677])] = {{182, 0}};
		mapped_strings[string_t(owned_strings[1678])] = {{8741, 0}};
		mapped_strings[string_t(owned_strings[1679])] = {{10995, 0}};
		mapped_strings[string_t(owned_strings[1680])] = {{11005, 0}};
		mapped_strings[string_t(owned_strings[1681])] = {{8706, 0}};
		mapped_strings[string_t(owned_strings[1682])] = {{1087, 0}};
		mapped_strings[string_t(owned_strings[1683])] = {{37, 0}};
		mapped_strings[string_t(owned_strings[1684])] = {{46, 0}};
		mapped_strings[string_t(owned_strings[1685])] = {{8240, 0}};
		mapped_strings[string_t(owned_strings[1686])] = {{8869, 0}};
		mapped_strings[string_t(owned_strings[1687])] = {{8241, 0}};
		mapped_strings[string_t(owned_strings[1688])] = {{120109, 0}};
		mapped_strings[string_t(owned_strings[1689])] = {{966, 0}};
		mapped_strings[string_t(owned_strings[1690])] = {{981, 0}};
		mapped_strings[string_t(owned_strings[1691])] = {{8499, 0}};
		mapped_strings[string_t(owned_strings[1692])] = {{9742, 0}};
		mapped_strings[string_t(owned_strings[1693])] = {{960, 0}};
		mapped_strings[string_t(owned_strings[1694])] = {{8916, 0}};
		mapped_strings[string_t(owned_strings[1695])] = {{982, 0}};
		mapped_strings[string_t(owned_strings[1696])] = {{8463, 0}};
		mapped_strings[string_t(owned_strings[1697])] = {{8462, 0}};
		mapped_strings[string_t(owned_strings[1698])] = {{8463, 0}};
		mapped_strings[string_t(owned_strings[1699])] = {{43, 0}};
		mapped_strings[string_t(owned_strings[1700])] = {{10787, 0}};
		mapped_strings[string_t(owned_strings[1701])] = {{8862, 0}};
		mapped_strings[string_t(owned_strings[1702])] = {{10786, 0}};
		mapped_strings[string_t(owned_strings[1703])] = {{8724, 0}};
		mapped_strings[string_t(owned_strings[1704])] = {{10789, 0}};
		mapped_strings[string_t(owned_strings[1705])] = {{10866, 0}};
		mapped_strings[string_t(owned_strings[1706])] = {{177, 0}};
		mapped_strings[string_t(owned_strings[1707])] = {{177, 0}};
		mapped_strings[string_t(owned_strings[1708])] = {{10790, 0}};
		mapped_strings[string_t(owned_strings[1709])] = {{10791, 0}};
		mapped_strings[string_t(owned_strings[1710])] = {{177, 0}};
		mapped_strings[string_t(owned_strings[1711])] = {{10773, 0}};
		mapped_strings[string_t(owned_strings[1712])] = {{120161, 0}};
		mapped_strings[string_t(owned_strings[1713])] = {{163, 0}};
		mapped_strings[string_t(owned_strings[1714])] = {{163, 0}};
		mapped_strings[string_t(owned_strings[1715])] = {{8826, 0}};
		mapped_strings[string_t(owned_strings[1716])] = {{10931, 0}};
		mapped_strings[string_t(owned_strings[1717])] = {{10935, 0}};
		mapped_strings[string_t(owned_strings[1718])] = {{8828, 0}};
		mapped_strings[string_t(owned_strings[1719])] = {{10927, 0}};
		mapped_strings[string_t(owned_strings[1720])] = {{8826, 0}};
		mapped_strings[string_t(owned_strings[1721])] = {{10935, 0}};
		mapped_strings[string_t(owned_strings[1722])] = {{8828, 0}};
		mapped_strings[string_t(owned_strings[1723])] = {{10927, 0}};
		mapped_strings[string_t(owned_strings[1724])] = {{10937, 0}};
		mapped_strings[string_t(owned_strings[1725])] = {{10933, 0}};
		mapped_strings[string_t(owned_strings[1726])] = {{8936, 0}};
		mapped_strings[string_t(owned_strings[1727])] = {{8830, 0}};
		mapped_strings[string_t(owned_strings[1728])] = {{8242, 0}};
		mapped_strings[string_t(owned_strings[1729])] = {{8473, 0}};
		mapped_strings[string_t(owned_strings[1730])] = {{10933, 0}};
		mapped_strings[string_t(owned_strings[1731])] = {{10937, 0}};
		mapped_strings[string_t(owned_strings[1732])] = {{8936, 0}};
		mapped_strings[string_t(owned_strings[1733])] = {{8719, 0}};
		mapped_strings[string_t(owned_strings[1734])] = {{9006, 0}};
		mapped_strings[string_t(owned_strings[1735])] = {{8978, 0}};
		mapped_strings[string_t(owned_strings[1736])] = {{8979, 0}};
		mapped_strings[string_t(owned_strings[1737])] = {{8733, 0}};
		mapped_strings[string_t(owned_strings[1738])] = {{8733, 0}};
		mapped_strings[string_t(owned_strings[1739])] = {{8830, 0}};
		mapped_strings[string_t(owned_strings[1740])] = {{8880, 0}};
		mapped_strings[string_t(owned_strings[1741])] = {{120005, 0}};
		mapped_strings[string_t(owned_strings[1742])] = {{968, 0}};
		mapped_strings[string_t(owned_strings[1743])] = {{8200, 0}};
		mapped_strings[string_t(owned_strings[1744])] = {{120110, 0}};
		mapped_strings[string_t(owned_strings[1745])] = {{10764, 0}};
		mapped_strings[string_t(owned_strings[1746])] = {{120162, 0}};
		mapped_strings[string_t(owned_strings[1747])] = {{8279, 0}};
		mapped_strings[string_t(owned_strings[1748])] = {{120006, 0}};
		mapped_strings[string_t(owned_strings[1749])] = {{8461, 0}};
		mapped_strings[string_t(owned_strings[1750])] = {{10774, 0}};
		mapped_strings[string_t(owned_strings[1751])] = {{63, 0}};
		mapped_strings[string_t(owned_strings[1752])] = {{8799, 0}};
		mapped_strings[string_t(owned_strings[1753])] = {{34, 0}};
		mapped_strings[string_t(owned_strings[1754])] = {{34, 0}};
		mapped_strings[string_t(owned_strings[1755])] = {{8667, 0}};
		mapped_strings[string_t(owned_strings[1756])] = {{8658, 0}};
		mapped_strings[string_t(owned_strings[1757])] = {{10524, 0}};
		mapped_strings[string_t(owned_strings[1758])] = {{10511, 0}};
		mapped_strings[string_t(owned_strings[1759])] = {{10596, 0}};
		mapped_strings[string_t(owned_strings[1760])] = {{8765, 817}};
		mapped_strings[string_t(owned_strings[1761])] = {{341, 0}};
		mapped_strings[string_t(owned_strings[1762])] = {{8730, 0}};
		mapped_strings[string_t(owned_strings[1763])] = {{10675, 0}};
		mapped_strings[string_t(owned_strings[1764])] = {{10217, 0}};
		mapped_strings[string_t(owned_strings[1765])] = {{10642, 0}};
		mapped_strings[string_t(owned_strings[1766])] = {{10661, 0}};
		mapped_strings[string_t(owned_strings[1767])] = {{10217, 0}};
		mapped_strings[string_t(owned_strings[1768])] = {{187, 0}};
		mapped_strings[string_t(owned_strings[1769])] = {{187, 0}};
		mapped_strings[string_t(owned_strings[1770])] = {{8594, 0}};
		mapped_strings[string_t(owned_strings[1771])] = {{10613, 0}};
		mapped_strings[string_t(owned_strings[1772])] = {{8677, 0}};
		mapped_strings[string_t(owned_strings[1773])] = {{10528, 0}};
		mapped_strings[string_t(owned_strings[1774])] = {{10547, 0}};
		mapped_strings[string_t(owned_strings[1775])] = {{10526, 0}};
		mapped_strings[string_t(owned_strings[1776])] = {{8618, 0}};
		mapped_strings[string_t(owned_strings[1777])] = {{8620, 0}};
		mapped_strings[string_t(owned_strings[1778])] = {{10565, 0}};
		mapped_strings[string_t(owned_strings[1779])] = {{10612, 0}};
		mapped_strings[string_t(owned_strings[1780])] = {{8611, 0}};
		mapped_strings[string_t(owned_strings[1781])] = {{8605, 0}};
		mapped_strings[string_t(owned_strings[1782])] = {{10522, 0}};
		mapped_strings[string_t(owned_strings[1783])] = {{8758, 0}};
		mapped_strings[string_t(owned_strings[1784])] = {{8474, 0}};
		mapped_strings[string_t(owned_strings[1785])] = {{10509, 0}};
		mapped_strings[string_t(owned_strings[1786])] = {{10099, 0}};
		mapped_strings[string_t(owned_strings[1787])] = {{125, 0}};
		mapped_strings[string_t(owned_strings[1788])] = {{93, 0}};
		mapped_strings[string_t(owned_strings[1789])] = {{10636, 0}};
		mapped_strings[string_t(owned_strings[1790])] = {{10638, 0}};
		mapped_strings[string_t(owned_strings[1791])] = {{10640, 0}};
		mapped_strings[string_t(owned_strings[1792])] = {{345, 0}};
		mapped_strings[string_t(owned_strings[1793])] = {{343, 0}};
		mapped_strings[string_t(owned_strings[1794])] = {{8969, 0}};
		mapped_strings[string_t(owned_strings[1795])] = {{125, 0}};
		mapped_strings[string_t(owned_strings[1796])] = {{1088, 0}};
		mapped_strings[string_t(owned_strings[1797])] = {{10551, 0}};
		mapped_strings[string_t(owned_strings[1798])] = {{10601, 0}};
		mapped_strings[string_t(owned_strings[1799])] = {{8221, 0}};
		mapped_strings[string_t(owned_strings[1800])] = {{8221, 0}};
		mapped_strings[string_t(owned_strings[1801])] = {{8627, 0}};
		mapped_strings[string_t(owned_strings[1802])] = {{8476, 0}};
		mapped_strings[string_t(owned_strings[1803])] = {{8475, 0}};
		mapped_strings[string_t(owned_strings[1804])] = {{8476, 0}};
		mapped_strings[string_t(owned_strings[1805])] = {{8477, 0}};
		mapped_strings[string_t(owned_strings[1806])] = {{9645, 0}};
		mapped_strings[string_t(owned_strings[1807])] = {{174, 0}};
		mapped_strings[string_t(owned_strings[1808])] = {{174, 0}};
		mapped_strings[string_t(owned_strings[1809])] = {{10621, 0}};
		mapped_strings[string_t(owned_strings[1810])] = {{8971, 0}};
		mapped_strings[string_t(owned_strings[1811])] = {{120111, 0}};
		mapped_strings[string_t(owned_strings[1812])] = {{8641, 0}};
		mapped_strings[string_t(owned_strings[1813])] = {{8640, 0}};
		mapped_strings[string_t(owned_strings[1814])] = {{10604, 0}};
		mapped_strings[string_t(owned_strings[1815])] = {{961, 0}};
		mapped_strings[string_t(owned_strings[1816])] = {{1009, 0}};
		mapped_strings[string_t(owned_strings[1817])] = {{8594, 0}};
		mapped_strings[string_t(owned_strings[1818])] = {{8611, 0}};
		mapped_strings[string_t(owned_strings[1819])] = {{8641, 0}};
		mapped_strings[string_t(owned_strings[1820])] = {{8640, 0}};
		mapped_strings[string_t(owned_strings[1821])] = {{8644, 0}};
		mapped_strings[string_t(owned_strings[1822])] = {{8652, 0}};
		mapped_strings[string_t(owned_strings[1823])] = {{8649, 0}};
		mapped_strings[string_t(owned_strings[1824])] = {{8605, 0}};
		mapped_strings[string_t(owned_strings[1825])] = {{8908, 0}};
		mapped_strings[string_t(owned_strings[1826])] = {{730, 0}};
		mapped_strings[string_t(owned_strings[1827])] = {{8787, 0}};
		mapped_strings[string_t(owned_strings[1828])] = {{8644, 0}};
		mapped_strings[string_t(owned_strings[1829])] = {{8652, 0}};
		mapped_strings[string_t(owned_strings[1830])] = {{8207, 0}};
		mapped_strings[string_t(owned_strings[1831])] = {{9137, 0}};
		mapped_strings[string_t(owned_strings[1832])] = {{9137, 0}};
		mapped_strings[string_t(owned_strings[1833])] = {{10990, 0}};
		mapped_strings[string_t(owned_strings[1834])] = {{10221, 0}};
		mapped_strings[string_t(owned_strings[1835])] = {{8702, 0}};
		mapped_strings[string_t(owned_strings[1836])] = {{10215, 0}};
		mapped_strings[string_t(owned_strings[1837])] = {{10630, 0}};
		mapped_strings[string_t(owned_strings[1838])] = {{120163, 0}};
		mapped_strings[string_t(owned_strings[1839])] = {{10798, 0}};
		mapped_strings[string_t(owned_strings[1840])] = {{10805, 0}};
		mapped_strings[string_t(owned_strings[1841])] = {{41, 0}};
		mapped_strings[string_t(owned_strings[1842])] = {{10644, 0}};
		mapped_strings[string_t(owned_strings[1843])] = {{10770, 0}};
		mapped_strings[string_t(owned_strings[1844])] = {{8649, 0}};
		mapped_strings[string_t(owned_strings[1845])] = {{8250, 0}};
		mapped_strings[string_t(owned_strings[1846])] = {{120007, 0}};
		mapped_strings[string_t(owned_strings[1847])] = {{8625, 0}};
		mapped_strings[string_t(owned_strings[1848])] = {{93, 0}};
		mapped_strings[string_t(owned_strings[1849])] = {{8217, 0}};
		mapped_strings[string_t(owned_strings[1850])] = {{8217, 0}};
		mapped_strings[string_t(owned_strings[1851])] = {{8908, 0}};
		mapped_strings[string_t(owned_strings[1852])] = {{8906, 0}};
		mapped_strings[string_t(owned_strings[1853])] = {{9657, 0}};
		mapped_strings[string_t(owned_strings[1854])] = {{8885, 0}};
		mapped_strings[string_t(owned_strings[1855])] = {{9656, 0}};
		mapped_strings[string_t(owned_strings[1856])] = {{10702, 0}};
		mapped_strings[string_t(owned_strings[1857])] = {{10600, 0}};
		mapped_strings[string_t(owned_strings[1858])] = {{8478, 0}};
		mapped_strings[string_t(owned_strings[1859])] = {{347, 0}};
		mapped_strings[string_t(owned_strings[1860])] = {{8218, 0}};
		mapped_strings[string_t(owned_strings[1861])] = {{8827, 0}};
		mapped_strings[string_t(owned_strings[1862])] = {{10932, 0}};
		mapped_strings[string_t(owned_strings[1863])] = {{10936, 0}};
		mapped_strings[string_t(owned_strings[1864])] = {{353, 0}};
		mapped_strings[string_t(owned_strings[1865])] = {{8829, 0}};
		mapped_strings[string_t(owned_strings[1866])] = {{10928, 0}};
		mapped_strings[string_t(owned_strings[1867])] = {{351, 0}};
		mapped_strings[string_t(owned_strings[1868])] = {{349, 0}};
		mapped_strings[string_t(owned_strings[1869])] = {{10934, 0}};
		mapped_strings[string_t(owned_strings[1870])] = {{10938, 0}};
		mapped_strings[string_t(owned_strings[1871])] = {{8937, 0}};
		mapped_strings[string_t(owned_strings[1872])] = {{10771, 0}};
		mapped_strings[string_t(owned_strings[1873])] = {{8831, 0}};
		mapped_strings[string_t(owned_strings[1874])] = {{1089, 0}};
		mapped_strings[string_t(owned_strings[1875])] = {{8901, 0}};
		mapped_strings[string_t(owned_strings[1876])] = {{8865, 0}};
		mapped_strings[string_t(owned_strings[1877])] = {{10854, 0}};
		mapped_strings[string_t(owned_strings[1878])] = {{8664, 0}};
		mapped_strings[string_t(owned_strings[1879])] = {{10533, 0}};
		mapped_strings[string_t(owned_strings[1880])] = {{8600, 0}};
		mapped_strings[string_t(owned_strings[1881])] = {{8600, 0}};
		mapped_strings[string_t(owned_strings[1882])] = {{167, 0}};
		mapped_strings[string_t(owned_strings[1883])] = {{167, 0}};
		mapped_strings[string_t(owned_strings[1884])] = {{59, 0}};
		mapped_strings[string_t(owned_strings[1885])] = {{10537, 0}};
		mapped_strings[string_t(owned_strings[1886])] = {{8726, 0}};
		mapped_strings[string_t(owned_strings[1887])] = {{8726, 0}};
		mapped_strings[string_t(owned_strings[1888])] = {{10038, 0}};
		mapped_strings[string_t(owned_strings[1889])] = {{120112, 0}};
		mapped_strings[string_t(owned_strings[1890])] = {{8994, 0}};
		mapped_strings[string_t(owned_strings[1891])] = {{9839, 0}};
		mapped_strings[string_t(owned_strings[1892])] = {{1097, 0}};
		mapped_strings[string_t(owned_strings[1893])] = {{1096, 0}};
		mapped_strings[string_t(owned_strings[1894])] = {{8739, 0}};
		mapped_strings[string_t(owned_strings[1895])] = {{8741, 0}};
		mapped_strings[string_t(owned_strings[1896])] = {{173, 0}};
		mapped_strings[string_t(owned_strings[1897])] = {{173, 0}};
		mapped_strings[string_t(owned_strings[1898])] = {{963, 0}};
		mapped_strings[string_t(owned_strings[1899])] = {{962, 0}};
		mapped_strings[string_t(owned_strings[1900])] = {{962, 0}};
		mapped_strings[string_t(owned_strings[1901])] = {{8764, 0}};
		mapped_strings[string_t(owned_strings[1902])] = {{10858, 0}};
		mapped_strings[string_t(owned_strings[1903])] = {{8771, 0}};
		mapped_strings[string_t(owned_strings[1904])] = {{8771, 0}};
		mapped_strings[string_t(owned_strings[1905])] = {{10910, 0}};
		mapped_strings[string_t(owned_strings[1906])] = {{10912, 0}};
		mapped_strings[string_t(owned_strings[1907])] = {{10909, 0}};
		mapped_strings[string_t(owned_strings[1908])] = {{10911, 0}};
		mapped_strings[string_t(owned_strings[1909])] = {{8774, 0}};
		mapped_strings[string_t(owned_strings[1910])] = {{10788, 0}};
		mapped_strings[string_t(owned_strings[1911])] = {{10610, 0}};
		mapped_strings[string_t(owned_strings[1912])] = {{8592, 0}};
		mapped_strings[string_t(owned_strings[1913])] = {{8726, 0}};
		mapped_strings[string_t(owned_strings[1914])] = {{10803, 0}};
		mapped_strings[string_t(owned_strings[1915])] = {{10724, 0}};
		mapped_strings[string_t(owned_strings[1916])] = {{8739, 0}};
		mapped_strings[string_t(owned_strings[1917])] = {{8995, 0}};
		mapped_strings[string_t(owned_strings[1918])] = {{10922, 0}};
		mapped_strings[string_t(owned_strings[1919])] = {{10924, 0}};
		mapped_strings[string_t(owned_strings[1920])] = {{10924, 65024}};
		mapped_strings[string_t(owned_strings[1921])] = {{1100, 0}};
		mapped_strings[string_t(owned_strings[1922])] = {{47, 0}};
		mapped_strings[string_t(owned_strings[1923])] = {{10692, 0}};
		mapped_strings[string_t(owned_strings[1924])] = {{9023, 0}};
		mapped_strings[string_t(owned_strings[1925])] = {{120164, 0}};
		mapped_strings[string_t(owned_strings[1926])] = {{9824, 0}};
		mapped_strings[string_t(owned_strings[1927])] = {{9824, 0}};
		mapped_strings[string_t(owned_strings[1928])] = {{8741, 0}};
		mapped_strings[string_t(owned_strings[1929])] = {{8851, 0}};
		mapped_strings[string_t(owned_strings[1930])] = {{8851, 65024}};
		mapped_strings[string_t(owned_strings[1931])] = {{8852, 0}};
		mapped_strings[string_t(owned_strings[1932])] = {{8852, 65024}};
		mapped_strings[string_t(owned_strings[1933])] = {{8847, 0}};
		mapped_strings[string_t(owned_strings[1934])] = {{8849, 0}};
		mapped_strings[string_t(owned_strings[1935])] = {{8847, 0}};
		mapped_strings[string_t(owned_strings[1936])] = {{8849, 0}};
		mapped_strings[string_t(owned_strings[1937])] = {{8848, 0}};
		mapped_strings[string_t(owned_strings[1938])] = {{8850, 0}};
		mapped_strings[string_t(owned_strings[1939])] = {{8848, 0}};
		mapped_strings[string_t(owned_strings[1940])] = {{8850, 0}};
		mapped_strings[string_t(owned_strings[1941])] = {{9633, 0}};
		mapped_strings[string_t(owned_strings[1942])] = {{9633, 0}};
		mapped_strings[string_t(owned_strings[1943])] = {{9642, 0}};
		mapped_strings[string_t(owned_strings[1944])] = {{9642, 0}};
		mapped_strings[string_t(owned_strings[1945])] = {{8594, 0}};
		mapped_strings[string_t(owned_strings[1946])] = {{120008, 0}};
		mapped_strings[string_t(owned_strings[1947])] = {{8726, 0}};
		mapped_strings[string_t(owned_strings[1948])] = {{8995, 0}};
		mapped_strings[string_t(owned_strings[1949])] = {{8902, 0}};
		mapped_strings[string_t(owned_strings[1950])] = {{9734, 0}};
		mapped_strings[string_t(owned_strings[1951])] = {{9733, 0}};
		mapped_strings[string_t(owned_strings[1952])] = {{1013, 0}};
		mapped_strings[string_t(owned_strings[1953])] = {{981, 0}};
		mapped_strings[string_t(owned_strings[1954])] = {{175, 0}};
		mapped_strings[string_t(owned_strings[1955])] = {{8834, 0}};
		mapped_strings[string_t(owned_strings[1956])] = {{10949, 0}};
		mapped_strings[string_t(owned_strings[1957])] = {{10941, 0}};
		mapped_strings[string_t(owned_strings[1958])] = {{8838, 0}};
		mapped_strings[string_t(owned_strings[1959])] = {{10947, 0}};
		mapped_strings[string_t(owned_strings[1960])] = {{10945, 0}};
		mapped_strings[string_t(owned_strings[1961])] = {{10955, 0}};
		mapped_strings[string_t(owned_strings[1962])] = {{8842, 0}};
		mapped_strings[string_t(owned_strings[1963])] = {{10943, 0}};
		mapped_strings[string_t(owned_strings[1964])] = {{10617, 0}};
		mapped_strings[string_t(owned_strings[1965])] = {{8834, 0}};
		mapped_strings[string_t(owned_strings[1966])] = {{8838, 0}};
		mapped_strings[string_t(owned_strings[1967])] = {{10949, 0}};
		mapped_strings[string_t(owned_strings[1968])] = {{8842, 0}};
		mapped_strings[string_t(owned_strings[1969])] = {{10955, 0}};
		mapped_strings[string_t(owned_strings[1970])] = {{10951, 0}};
		mapped_strings[string_t(owned_strings[1971])] = {{10965, 0}};
		mapped_strings[string_t(owned_strings[1972])] = {{10963, 0}};
		mapped_strings[string_t(owned_strings[1973])] = {{8827, 0}};
		mapped_strings[string_t(owned_strings[1974])] = {{10936, 0}};
		mapped_strings[string_t(owned_strings[1975])] = {{8829, 0}};
		mapped_strings[string_t(owned_strings[1976])] = {{10928, 0}};
		mapped_strings[string_t(owned_strings[1977])] = {{10938, 0}};
		mapped_strings[string_t(owned_strings[1978])] = {{10934, 0}};
		mapped_strings[string_t(owned_strings[1979])] = {{8937, 0}};
		mapped_strings[string_t(owned_strings[1980])] = {{8831, 0}};
		mapped_strings[string_t(owned_strings[1981])] = {{8721, 0}};
		mapped_strings[string_t(owned_strings[1982])] = {{9834, 0}};
		mapped_strings[string_t(owned_strings[1983])] = {{185, 0}};
		mapped_strings[string_t(owned_strings[1984])] = {{185, 0}};
		mapped_strings[string_t(owned_strings[1985])] = {{178, 0}};
		mapped_strings[string_t(owned_strings[1986])] = {{178, 0}};
		mapped_strings[string_t(owned_strings[1987])] = {{179, 0}};
		mapped_strings[string_t(owned_strings[1988])] = {{179, 0}};
		mapped_strings[string_t(owned_strings[1989])] = {{8835, 0}};
		mapped_strings[string_t(owned_strings[1990])] = {{10950, 0}};
		mapped_strings[string_t(owned_strings[1991])] = {{10942, 0}};
		mapped_strings[string_t(owned_strings[1992])] = {{10968, 0}};
		mapped_strings[string_t(owned_strings[1993])] = {{8839, 0}};
		mapped_strings[string_t(owned_strings[1994])] = {{10948, 0}};
		mapped_strings[string_t(owned_strings[1995])] = {{10185, 0}};
		mapped_strings[string_t(owned_strings[1996])] = {{10967, 0}};
		mapped_strings[string_t(owned_strings[1997])] = {{10619, 0}};
		mapped_strings[string_t(owned_strings[1998])] = {{10946, 0}};
		mapped_strings[string_t(owned_strings[1999])] = {{10956, 0}};
		mapped_strings[string_t(owned_strings[2000])] = {{8843, 0}};
		mapped_strings[string_t(owned_strings[2001])] = {{10944, 0}};
		mapped_strings[string_t(owned_strings[2002])] = {{8835, 0}};
		mapped_strings[string_t(owned_strings[2003])] = {{8839, 0}};
		mapped_strings[string_t(owned_strings[2004])] = {{10950, 0}};
		mapped_strings[string_t(owned_strings[2005])] = {{8843, 0}};
		mapped_strings[string_t(owned_strings[2006])] = {{10956, 0}};
		mapped_strings[string_t(owned_strings[2007])] = {{10952, 0}};
		mapped_strings[string_t(owned_strings[2008])] = {{10964, 0}};
		mapped_strings[string_t(owned_strings[2009])] = {{10966, 0}};
		mapped_strings[string_t(owned_strings[2010])] = {{8665, 0}};
		mapped_strings[string_t(owned_strings[2011])] = {{10534, 0}};
		mapped_strings[string_t(owned_strings[2012])] = {{8601, 0}};
		mapped_strings[string_t(owned_strings[2013])] = {{8601, 0}};
		mapped_strings[string_t(owned_strings[2014])] = {{10538, 0}};
		mapped_strings[string_t(owned_strings[2015])] = {{223, 0}};
		mapped_strings[string_t(owned_strings[2016])] = {{223, 0}};
		mapped_strings[string_t(owned_strings[2017])] = {{8982, 0}};
		mapped_strings[string_t(owned_strings[2018])] = {{964, 0}};
		mapped_strings[string_t(owned_strings[2019])] = {{9140, 0}};
		mapped_strings[string_t(owned_strings[2020])] = {{357, 0}};
		mapped_strings[string_t(owned_strings[2021])] = {{355, 0}};
		mapped_strings[string_t(owned_strings[2022])] = {{1090, 0}};
		mapped_strings[string_t(owned_strings[2023])] = {{8411, 0}};
		mapped_strings[string_t(owned_strings[2024])] = {{8981, 0}};
		mapped_strings[string_t(owned_strings[2025])] = {{120113, 0}};
		mapped_strings[string_t(owned_strings[2026])] = {{8756, 0}};
		mapped_strings[string_t(owned_strings[2027])] = {{8756, 0}};
		mapped_strings[string_t(owned_strings[2028])] = {{952, 0}};
		mapped_strings[string_t(owned_strings[2029])] = {{977, 0}};
		mapped_strings[string_t(owned_strings[2030])] = {{977, 0}};
		mapped_strings[string_t(owned_strings[2031])] = {{8776, 0}};
		mapped_strings[string_t(owned_strings[2032])] = {{8764, 0}};
		mapped_strings[string_t(owned_strings[2033])] = {{8201, 0}};
		mapped_strings[string_t(owned_strings[2034])] = {{8776, 0}};
		mapped_strings[string_t(owned_strings[2035])] = {{8764, 0}};
		mapped_strings[string_t(owned_strings[2036])] = {{254, 0}};
		mapped_strings[string_t(owned_strings[2037])] = {{254, 0}};
		mapped_strings[string_t(owned_strings[2038])] = {{732, 0}};
		mapped_strings[string_t(owned_strings[2039])] = {{215, 0}};
		mapped_strings[string_t(owned_strings[2040])] = {{215, 0}};
		mapped_strings[string_t(owned_strings[2041])] = {{8864, 0}};
		mapped_strings[string_t(owned_strings[2042])] = {{10801, 0}};
		mapped_strings[string_t(owned_strings[2043])] = {{10800, 0}};
		mapped_strings[string_t(owned_strings[2044])] = {{8749, 0}};
		mapped_strings[string_t(owned_strings[2045])] = {{10536, 0}};
		mapped_strings[string_t(owned_strings[2046])] = {{8868, 0}};
		mapped_strings[string_t(owned_strings[2047])] = {{9014, 0}};
		mapped_strings[string_t(owned_strings[2048])] = {{10993, 0}};
		mapped_strings[string_t(owned_strings[2049])] = {{120165, 0}};
		mapped_strings[string_t(owned_strings[2050])] = {{10970, 0}};
		mapped_strings[string_t(owned_strings[2051])] = {{10537, 0}};
		mapped_strings[string_t(owned_strings[2052])] = {{8244, 0}};
		mapped_strings[string_t(owned_strings[2053])] = {{8482, 0}};
		mapped_strings[string_t(owned_strings[2054])] = {{9653, 0}};
		mapped_strings[string_t(owned_strings[2055])] = {{9663, 0}};
		mapped_strings[string_t(owned_strings[2056])] = {{9667, 0}};
		mapped_strings[string_t(owned_strings[2057])] = {{8884, 0}};
		mapped_strings[string_t(owned_strings[2058])] = {{8796, 0}};
		mapped_strings[string_t(owned_strings[2059])] = {{9657, 0}};
		mapped_strings[string_t(owned_strings[2060])] = {{8885, 0}};
		mapped_strings[string_t(owned_strings[2061])] = {{9708, 0}};
		mapped_strings[string_t(owned_strings[2062])] = {{8796, 0}};
		mapped_strings[string_t(owned_strings[2063])] = {{10810, 0}};
		mapped_strings[string_t(owned_strings[2064])] = {{10809, 0}};
		mapped_strings[string_t(owned_strings[2065])] = {{10701, 0}};
		mapped_strings[string_t(owned_strings[2066])] = {{10811, 0}};
		mapped_strings[string_t(owned_strings[2067])] = {{9186, 0}};
		mapped_strings[string_t(owned_strings[2068])] = {{120009, 0}};
		mapped_strings[string_t(owned_strings[2069])] = {{1094, 0}};
		mapped_strings[string_t(owned_strings[2070])] = {{1115, 0}};
		mapped_strings[string_t(owned_strings[2071])] = {{359, 0}};
		mapped_strings[string_t(owned_strings[2072])] = {{8812, 0}};
		mapped_strings[string_t(owned_strings[2073])] = {{8606, 0}};
		mapped_strings[string_t(owned_strings[2074])] = {{8608, 0}};
		mapped_strings[string_t(owned_strings[2075])] = {{8657, 0}};
		mapped_strings[string_t(owned_strings[2076])] = {{10595, 0}};
		mapped_strings[string_t(owned_strings[2077])] = {{250, 0}};
		mapped_strings[string_t(owned_strings[2078])] = {{250, 0}};
		mapped_strings[string_t(owned_strings[2079])] = {{8593, 0}};
		mapped_strings[string_t(owned_strings[2080])] = {{1118, 0}};
		mapped_strings[string_t(owned_strings[2081])] = {{365, 0}};
		mapped_strings[string_t(owned_strings[2082])] = {{251, 0}};
		mapped_strings[string_t(owned_strings[2083])] = {{251, 0}};
		mapped_strings[string_t(owned_strings[2084])] = {{1091, 0}};
		mapped_strings[string_t(owned_strings[2085])] = {{8645, 0}};
		mapped_strings[string_t(owned_strings[2086])] = {{369, 0}};
		mapped_strings[string_t(owned_strings[2087])] = {{10606, 0}};
		mapped_strings[string_t(owned_strings[2088])] = {{10622, 0}};
		mapped_strings[string_t(owned_strings[2089])] = {{120114, 0}};
		mapped_strings[string_t(owned_strings[2090])] = {{249, 0}};
		mapped_strings[string_t(owned_strings[2091])] = {{249, 0}};
		mapped_strings[string_t(owned_strings[2092])] = {{8639, 0}};
		mapped_strings[string_t(owned_strings[2093])] = {{8638, 0}};
		mapped_strings[string_t(owned_strings[2094])] = {{9600, 0}};
		mapped_strings[string_t(owned_strings[2095])] = {{8988, 0}};
		mapped_strings[string_t(owned_strings[2096])] = {{8988, 0}};
		mapped_strings[string_t(owned_strings[2097])] = {{8975, 0}};
		mapped_strings[string_t(owned_strings[2098])] = {{9720, 0}};
		mapped_strings[string_t(owned_strings[2099])] = {{363, 0}};
		mapped_strings[string_t(owned_strings[2100])] = {{168, 0}};
		mapped_strings[string_t(owned_strings[2101])] = {{168, 0}};
		mapped_strings[string_t(owned_strings[2102])] = {{371, 0}};
		mapped_strings[string_t(owned_strings[2103])] = {{120166, 0}};
		mapped_strings[string_t(owned_strings[2104])] = {{8593, 0}};
		mapped_strings[string_t(owned_strings[2105])] = {{8597, 0}};
		mapped_strings[string_t(owned_strings[2106])] = {{8639, 0}};
		mapped_strings[string_t(owned_strings[2107])] = {{8638, 0}};
		mapped_strings[string_t(owned_strings[2108])] = {{8846, 0}};
		mapped_strings[string_t(owned_strings[2109])] = {{965, 0}};
		mapped_strings[string_t(owned_strings[2110])] = {{978, 0}};
		mapped_strings[string_t(owned_strings[2111])] = {{965, 0}};
		mapped_strings[string_t(owned_strings[2112])] = {{8648, 0}};
		mapped_strings[string_t(owned_strings[2113])] = {{8989, 0}};
		mapped_strings[string_t(owned_strings[2114])] = {{8989, 0}};
		mapped_strings[string_t(owned_strings[2115])] = {{8974, 0}};
		mapped_strings[string_t(owned_strings[2116])] = {{367, 0}};
		mapped_strings[string_t(owned_strings[2117])] = {{9721, 0}};
		mapped_strings[string_t(owned_strings[2118])] = {{120010, 0}};
		mapped_strings[string_t(owned_strings[2119])] = {{8944, 0}};
		mapped_strings[string_t(owned_strings[2120])] = {{361, 0}};
		mapped_strings[string_t(owned_strings[2121])] = {{9653, 0}};
		mapped_strings[string_t(owned_strings[2122])] = {{9652, 0}};
		mapped_strings[string_t(owned_strings[2123])] = {{8648, 0}};
		mapped_strings[string_t(owned_strings[2124])] = {{252, 0}};
		mapped_strings[string_t(owned_strings[2125])] = {{252, 0}};
		mapped_strings[string_t(owned_strings[2126])] = {{10663, 0}};
		mapped_strings[string_t(owned_strings[2127])] = {{8661, 0}};
		mapped_strings[string_t(owned_strings[2128])] = {{10984, 0}};
		mapped_strings[string_t(owned_strings[2129])] = {{10985, 0}};
		mapped_strings[string_t(owned_strings[2130])] = {{8872, 0}};
		mapped_strings[string_t(owned_strings[2131])] = {{10652, 0}};
		mapped_strings[string_t(owned_strings[2132])] = {{1013, 0}};
		mapped_strings[string_t(owned_strings[2133])] = {{1008, 0}};
		mapped_strings[string_t(owned_strings[2134])] = {{8709, 0}};
		mapped_strings[string_t(owned_strings[2135])] = {{981, 0}};
		mapped_strings[string_t(owned_strings[2136])] = {{982, 0}};
		mapped_strings[string_t(owned_strings[2137])] = {{8733, 0}};
		mapped_strings[string_t(owned_strings[2138])] = {{8597, 0}};
		mapped_strings[string_t(owned_strings[2139])] = {{1009, 0}};
		mapped_strings[string_t(owned_strings[2140])] = {{962, 0}};
		mapped_strings[string_t(owned_strings[2141])] = {{8842, 65024}};
		mapped_strings[string_t(owned_strings[2142])] = {{10955, 65024}};
		mapped_strings[string_t(owned_strings[2143])] = {{8843, 65024}};
		mapped_strings[string_t(owned_strings[2144])] = {{10956, 65024}};
		mapped_strings[string_t(owned_strings[2145])] = {{977, 0}};
		mapped_strings[string_t(owned_strings[2146])] = {{8882, 0}};
		mapped_strings[string_t(owned_strings[2147])] = {{8883, 0}};
		mapped_strings[string_t(owned_strings[2148])] = {{1074, 0}};
		mapped_strings[string_t(owned_strings[2149])] = {{8866, 0}};
		mapped_strings[string_t(owned_strings[2150])] = {{8744, 0}};
		mapped_strings[string_t(owned_strings[2151])] = {{8891, 0}};
		mapped_strings[string_t(owned_strings[2152])] = {{8794, 0}};
		mapped_strings[string_t(owned_strings[2153])] = {{8942, 0}};
		mapped_strings[string_t(owned_strings[2154])] = {{124, 0}};
		mapped_strings[string_t(owned_strings[2155])] = {{124, 0}};
		mapped_strings[string_t(owned_strings[2156])] = {{120115, 0}};
		mapped_strings[string_t(owned_strings[2157])] = {{8882, 0}};
		mapped_strings[string_t(owned_strings[2158])] = {{8834, 8402}};
		mapped_strings[string_t(owned_strings[2159])] = {{8835, 8402}};
		mapped_strings[string_t(owned_strings[2160])] = {{120167, 0}};
		mapped_strings[string_t(owned_strings[2161])] = {{8733, 0}};
		mapped_strings[string_t(owned_strings[2162])] = {{8883, 0}};
		mapped_strings[string_t(owned_strings[2163])] = {{120011, 0}};
		mapped_strings[string_t(owned_strings[2164])] = {{10955, 65024}};
		mapped_strings[string_t(owned_strings[2165])] = {{8842, 65024}};
		mapped_strings[string_t(owned_strings[2166])] = {{10956, 65024}};
		mapped_strings[string_t(owned_strings[2167])] = {{8843, 65024}};
		mapped_strings[string_t(owned_strings[2168])] = {{10650, 0}};
		mapped_strings[string_t(owned_strings[2169])] = {{373, 0}};
		mapped_strings[string_t(owned_strings[2170])] = {{10847, 0}};
		mapped_strings[string_t(owned_strings[2171])] = {{8743, 0}};
		mapped_strings[string_t(owned_strings[2172])] = {{8793, 0}};
		mapped_strings[string_t(owned_strings[2173])] = {{8472, 0}};
		mapped_strings[string_t(owned_strings[2174])] = {{120116, 0}};
		mapped_strings[string_t(owned_strings[2175])] = {{120168, 0}};
		mapped_strings[string_t(owned_strings[2176])] = {{8472, 0}};
		mapped_strings[string_t(owned_strings[2177])] = {{8768, 0}};
		mapped_strings[string_t(owned_strings[2178])] = {{8768, 0}};
		mapped_strings[string_t(owned_strings[2179])] = {{120012, 0}};
		mapped_strings[string_t(owned_strings[2180])] = {{8898, 0}};
		mapped_strings[string_t(owned_strings[2181])] = {{9711, 0}};
		mapped_strings[string_t(owned_strings[2182])] = {{8899, 0}};
		mapped_strings[string_t(owned_strings[2183])] = {{9661, 0}};
		mapped_strings[string_t(owned_strings[2184])] = {{120117, 0}};
		mapped_strings[string_t(owned_strings[2185])] = {{10234, 0}};
		mapped_strings[string_t(owned_strings[2186])] = {{10231, 0}};
		mapped_strings[string_t(owned_strings[2187])] = {{958, 0}};
		mapped_strings[string_t(owned_strings[2188])] = {{10232, 0}};
		mapped_strings[string_t(owned_strings[2189])] = {{10229, 0}};
		mapped_strings[string_t(owned_strings[2190])] = {{10236, 0}};
		mapped_strings[string_t(owned_strings[2191])] = {{8955, 0}};
		mapped_strings[string_t(owned_strings[2192])] = {{10752, 0}};
		mapped_strings[string_t(owned_strings[2193])] = {{120169, 0}};
		mapped_strings[string_t(owned_strings[2194])] = {{10753, 0}};
		mapped_strings[string_t(owned_strings[2195])] = {{10754, 0}};
		mapped_strings[string_t(owned_strings[2196])] = {{10233, 0}};
		mapped_strings[string_t(owned_strings[2197])] = {{10230, 0}};
		mapped_strings[string_t(owned_strings[2198])] = {{120013, 0}};
		mapped_strings[string_t(owned_strings[2199])] = {{10758, 0}};
		mapped_strings[string_t(owned_strings[2200])] = {{10756, 0}};
		mapped_strings[string_t(owned_strings[2201])] = {{9651, 0}};
		mapped_strings[string_t(owned_strings[2202])] = {{8897, 0}};
		mapped_strings[string_t(owned_strings[2203])] = {{8896, 0}};
		mapped_strings[string_t(owned_strings[2204])] = {{253, 0}};
		mapped_strings[string_t(owned_strings[2205])] = {{253, 0}};
		mapped_strings[string_t(owned_strings[2206])] = {{1103, 0}};
		mapped_strings[string_t(owned_strings[2207])] = {{375, 0}};
		mapped_strings[string_t(owned_strings[2208])] = {{1099, 0}};
		mapped_strings[string_t(owned_strings[2209])] = {{165, 0}};
		mapped_strings[string_t(owned_strings[2210])] = {{165, 0}};
		mapped_strings[string_t(owned_strings[2211])] = {{120118, 0}};
		mapped_strings[string_t(owned_strings[2212])] = {{1111, 0}};
		mapped_strings[string_t(owned_strings[2213])] = {{120170, 0}};
		mapped_strings[string_t(owned_strings[2214])] = {{120014, 0}};
		mapped_strings[string_t(owned_strings[2215])] = {{1102, 0}};
		mapped_strings[string_t(owned_strings[2216])] = {{255, 0}};
		mapped_strings[string_t(owned_strings[2217])] = {{255, 0}};
		mapped_strings[string_t(owned_strings[2218])] = {{378, 0}};
		mapped_strings[string_t(owned_strings[2219])] = {{382, 0}};
		mapped_strings[string_t(owned_strings[2220])] = {{1079, 0}};
		mapped_strings[string_t(owned_strings[2221])] = {{380, 0}};
		mapped_strings[string_t(owned_strings[2222])] = {{8488, 0}};
		mapped_strings[string_t(owned_strings[2223])] = {{950, 0}};
		mapped_strings[string_t(owned_strings[2224])] = {{120119, 0}};
		mapped_strings[string_t(owned_strings[2225])] = {{1078, 0}};
		mapped_strings[string_t(owned_strings[2226])] = {{8669, 0}};
		mapped_strings[string_t(owned_strings[2227])] = {{120171, 0}};
		mapped_strings[string_t(owned_strings[2228])] = {{120015, 0}};
		mapped_strings[string_t(owned_strings[2229])] = {{8205, 0}};
		mapped_strings[string_t(owned_strings[2230])] = {{8204, 0}};
		return mapped_strings;
	}

private:
	static const vector<string> owned_strings;
};

// definition of the static members
const vector<string> HTML5NameCharrefs::owned_strings = HTML5NameCharrefs::initialize_vector();
const string_map_t<HTMLEscapeCodepoint> HTML5NameCharrefs::mapped_strings = HTML5NameCharrefs::initialize_map();

} // namespace duckdb
