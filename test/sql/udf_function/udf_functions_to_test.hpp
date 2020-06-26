/*HEADER file with all UDF Functions to test*/
#pragma once

namespace duckdb {

//UDF Functions to test
bool udf_bool(bool a) {return a;}
bool udf_bool(bool a, bool b) {return a & b;}
bool udf_bool(bool a, bool b, bool c) {return a & b & c;}

int8_t udf_int8(int8_t a) {return a;}
int8_t udf_int8(int8_t a, int8_t b) {return a * b;}
int8_t udf_int8(int8_t a, int8_t b, int8_t c) {return a + b + c;}

int16_t udf_int16(int16_t a) {return a;}
int16_t udf_int16(int16_t a, int16_t b) {return a * b;}
int16_t udf_int16(int16_t a, int16_t b, int16_t c) {return a + b + c;}

date_t udf_date(date_t a) {return a;}
date_t udf_date(date_t a, date_t b) {return b;}
date_t udf_date(date_t a, date_t b, date_t c) {return c;}

dtime_t udf_time(dtime_t a) {return a;}
dtime_t udf_time(dtime_t a, dtime_t b) {return b;}
dtime_t udf_time(dtime_t a, dtime_t b, dtime_t c) {return c;}

int udf_int(int a) {return a;}
int udf_int(int a, int b) {return a * b;}
int udf_int(int a, int b, int c) {return a + b + c;}

int64_t udf_int64(int64_t a) {return a;}
int64_t udf_int64(int64_t a, int64_t b) {return a * b;}
int64_t udf_int64(int64_t a, int64_t b, int64_t c) {return a + b + c;}

timestamp_t udf_timestamp(timestamp_t a) {return a;}
timestamp_t udf_timestamp(timestamp_t a, timestamp_t b) {return b;}
timestamp_t udf_timestamp(timestamp_t a, timestamp_t b, timestamp_t c) {return c;}

float udf_float(float a) {return a;}
float udf_float(float a, float b) {return a * b;}
float udf_float(float a, float b, float c) {return a + b + c;}

double udf_double(double a) {return a;}
double udf_double(double a, double b) {return a * b;}
double udf_double(double a, double b, double c) {return a + b + c;}

double udf_decimal(double a) {return a;}
double udf_decimal(double a, double b) {return a * b;}
double udf_decimal(double a, double b, double c) {return a + b + c;}

string_t udf_varchar(string_t a) {return a;}
string_t udf_varchar(string_t a, string_t b) {return b;}
string_t udf_varchar(string_t a, string_t b, string_t c) {return c;}

}; //end namespace
