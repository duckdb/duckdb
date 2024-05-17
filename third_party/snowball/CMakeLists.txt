cmake_minimum_required(VERSION 2.8.12...3.29)

project(Snowball)

# cmake_policy(SET CMP0063 NEW)

include_directories(libstemmer runtime src_c)

add_library(
  snowball STATIC
  libstemmer/libstemmer.c
  runtime/utilities.c
  runtime/api.c
  src_c/stem_UTF_8_arabic.c
  src_c/stem_UTF_8_basque.c
  src_c/stem_UTF_8_catalan.c
  src_c/stem_UTF_8_danish.c
  src_c/stem_UTF_8_dutch.c
  src_c/stem_UTF_8_english.c
  src_c/stem_UTF_8_finnish.c
  src_c/stem_UTF_8_french.c
  src_c/stem_UTF_8_german.c
  src_c/stem_UTF_8_german2.c
  src_c/stem_UTF_8_greek.c
  src_c/stem_UTF_8_hindi.c
  src_c/stem_UTF_8_hungarian.c
  src_c/stem_UTF_8_indonesian.c
  src_c/stem_UTF_8_irish.c
  src_c/stem_UTF_8_italian.c
  src_c/stem_UTF_8_kraaij_pohlmann.c
  src_c/stem_UTF_8_lithuanian.c
  src_c/stem_UTF_8_lovins.c
  src_c/stem_UTF_8_nepali.c
  src_c/stem_UTF_8_norwegian.c
  src_c/stem_UTF_8_porter.c
  src_c/stem_UTF_8_portuguese.c
  src_c/stem_UTF_8_romanian.c
  src_c/stem_UTF_8_russian.c
  src_c/stem_UTF_8_serbian.c
  src_c/stem_UTF_8_spanish.c
  src_c/stem_UTF_8_swedish.c
  src_c/stem_UTF_8_tamil.c
  src_c/stem_UTF_8_turkish.c)

target_include_directories(
  snowball PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

install(
  TARGETS snowball
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
