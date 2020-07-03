/*HEADER file with all UDF Functions to test*/
#pragma once

namespace duckdb {

//UDF Functions to test
bool udf_bool_1(bool a) {return a;}
bool udf_bool_2(bool a, bool b) {return a & b;}
bool udf_bool_3(bool a, bool b, bool c) {return a & b & c;}

int8_t udf_int8_1(int8_t a) {return a;}
int8_t udf_int8_2(int8_t a, int8_t b) {return a * b;}
int8_t udf_int8_3(int8_t a, int8_t b, int8_t c) {return a + b + c;}

int16_t udf_int16_1(int16_t a) {return a;}
int16_t udf_int16_2(int16_t a, int16_t b) {return a * b;}
int16_t udf_int16_3(int16_t a, int16_t b, int16_t c) {return a + b + c;}

date_t udf_date_1(date_t a) {return a;}
date_t udf_date_2(date_t a, date_t b) {return b;}
date_t udf_date_3(date_t a, date_t b, date_t c) {return c;}

dtime_t udf_time_1(dtime_t a) {return a;}
dtime_t udf_time_2(dtime_t a, dtime_t b) {return b;}
dtime_t udf_time_3(dtime_t a, dtime_t b, dtime_t c) {return c;}

int udf_int_1(int a) {return a;}
int udf_int_2(int a, int b) {return a * b;}
int udf_int_3(int a, int b, int c) {return a + b + c;}

int64_t udf_int64_1(int64_t a) {return a;}
int64_t udf_int64_2(int64_t a, int64_t b) {return a * b;}
int64_t udf_int64_3(int64_t a, int64_t b, int64_t c) {return a + b + c;}

timestamp_t udf_timestamp_1(timestamp_t a) {return a;}
timestamp_t udf_timestamp_2(timestamp_t a, timestamp_t b) {return b;}
timestamp_t udf_timestamp_3(timestamp_t a, timestamp_t b, timestamp_t c) {return c;}

float udf_float_1(float a) {return a;}
float udf_float_2(float a, float b) {return a * b;}
float udf_float_3(float a, float b, float c) {return a + b + c;}

double udf_double_1(double a) {return a;}
double udf_double_2(double a, double b) {return a * b;}
double udf_double_3(double a, double b, double c) {return a + b + c;}

double udf_decimal_1(double a) {return a;}
double udf_decimal_2(double a, double b) {return a * b;}
double udf_decimal_3(double a, double b, double c) {return a + b + c;}

string_t udf_varchar_1(string_t a) {return a;}
string_t udf_varchar_2(string_t a, string_t b) {return b;}
string_t udf_varchar_3(string_t a, string_t b, string_t c) {return c;}

}; //end namespace
