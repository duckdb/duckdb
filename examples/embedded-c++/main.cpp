#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include "duckdb.hpp"

using namespace duckdb;

int load_partsupp(DuckDB db, Connection con) {
	string fname ("../../../data/partsupp.csv");
	con.Query("CREATE TABLE partsupp (PS_PARTKEY int, PS_SUPPKEY int, PS_AVAILQTY int, PS_SUPPLYCOST float);");
	vector<vector<string>> content; 
	vector<string> row;
	string line, word;
	
	Appender appender(con, "partsupp"); 	
	std::fstream file (fname, std::ios::in); 
	if(file.is_open())
	{
		while(getline(file, line))
		{
			row.clear(); 
			std::stringstream str(line); 
			while(getline(str, word, '|'))
				row.push_back(word);
			content.push_back(row);
		}
	}

	for(int i=0; i<content.size(); i++)
	{
	        int32_t key = std::stoi(content[i][0]); 	
	        int32_t supp_key = std::stoi(content[i][1]); 
		int32_t avail = std::stoi(content[i][2]); 
		float cost = std::stof(content[i][3]);
		//std::cout << typeid(content[i][4]).name() << std::endl; 	
		string comment = content[i][4]; 	
		appender.AppendRow(key, supp_key, avail, cost); 
	}
	appender.Close(); 

	return 0; 
}

int load_test(DuckDB db, Connection con){
	con.Query("CREATE TABLE people(id INTEGER, name VARCHAR)");
	Appender appender(con, "people");
	appender.BeginRow();
	appender.Append<int32_t>(2);
	appender.Append<string>("Hannes");
	appender.EndRow();
	appender.Flush(); 
	return 0; 
}
int main() {
	DuckDB db(nullptr);
	Connection con(db);
	//load_test(db, con); 
	load_partsupp(db, con);
	//initialize the appender

	auto result = con.Query("SELECT * FROM partsupp limit 10;");
	result->Print();
}
