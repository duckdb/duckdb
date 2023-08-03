import os

# list all include directories
include_directories = [
    os.path.sep.join(x.split('/'))
    for x in [
        'extension/fts/include',
        'third_party/snowball/libstemmer',
        'third_party/snowball/runtime',
        'third_party/snowball/src_c',
    ]
]
# source files
source_files = [
    os.path.sep.join(x.split('/')) for x in ['extension/fts/fts_extension.cpp', 'extension/fts/fts_indexing.cpp']
]
# snowball
source_files += [
    os.path.sep.join(x.split('/'))
    for x in [
        'third_party/snowball/libstemmer/libstemmer.cpp',
        'third_party/snowball/runtime/utilities.cpp',
        'third_party/snowball/runtime/api.cpp',
        'third_party/snowball/src_c/stem_UTF_8_arabic.cpp',
        'third_party/snowball/src_c/stem_UTF_8_basque.cpp',
        'third_party/snowball/src_c/stem_UTF_8_catalan.cpp',
        'third_party/snowball/src_c/stem_UTF_8_danish.cpp',
        'third_party/snowball/src_c/stem_UTF_8_dutch.cpp',
        'third_party/snowball/src_c/stem_UTF_8_english.cpp',
        'third_party/snowball/src_c/stem_UTF_8_finnish.cpp',
        'third_party/snowball/src_c/stem_UTF_8_french.cpp',
        'third_party/snowball/src_c/stem_UTF_8_german.cpp',
        'third_party/snowball/src_c/stem_UTF_8_german2.cpp',
        'third_party/snowball/src_c/stem_UTF_8_greek.cpp',
        'third_party/snowball/src_c/stem_UTF_8_hindi.cpp',
        'third_party/snowball/src_c/stem_UTF_8_hungarian.cpp',
        'third_party/snowball/src_c/stem_UTF_8_indonesian.cpp',
        'third_party/snowball/src_c/stem_UTF_8_irish.cpp',
        'third_party/snowball/src_c/stem_UTF_8_italian.cpp',
        'third_party/snowball/src_c/stem_UTF_8_kraaij_pohlmann.cpp',
        'third_party/snowball/src_c/stem_UTF_8_lithuanian.cpp',
        'third_party/snowball/src_c/stem_UTF_8_lovins.cpp',
        'third_party/snowball/src_c/stem_UTF_8_nepali.cpp',
        'third_party/snowball/src_c/stem_UTF_8_norwegian.cpp',
        'third_party/snowball/src_c/stem_UTF_8_porter.cpp',
        'third_party/snowball/src_c/stem_UTF_8_portuguese.cpp',
        'third_party/snowball/src_c/stem_UTF_8_romanian.cpp',
        'third_party/snowball/src_c/stem_UTF_8_russian.cpp',
        'third_party/snowball/src_c/stem_UTF_8_serbian.cpp',
        'third_party/snowball/src_c/stem_UTF_8_spanish.cpp',
        'third_party/snowball/src_c/stem_UTF_8_swedish.cpp',
        'third_party/snowball/src_c/stem_UTF_8_tamil.cpp',
        'third_party/snowball/src_c/stem_UTF_8_turkish.cpp',
    ]
]
