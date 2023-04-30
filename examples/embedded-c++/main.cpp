#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include "duckdb.hpp"

using namespace duckdb;

int load_partsupp(DuckDB) {
	string fname ("../../../data/partsupp.csv"); 
	vector<vector<string>> content; 
	vector<string> row;
	string line, word; 
	
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



	return 0; 
}
int main() {
	DuckDB db(nullptr);
	string fname("../../../data/partsupp.csv"); 
	vector<vector<string>> content; 
	vector<string> row;
	string line, word;

	std::fstream file (fname, std::ios::in);
	load_partsupp(db); 
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
	else
		std::cout<<"Could not open the file\n";
	for(int i=0;i<9;i++)
	{
		for(int j=0;j<content[i].size();j++)
		{
			std::cout<<content[i][j]<<", ";
		}
		std::cout<<"\n";
	}
	Connection con(db);
	con.Query("CREATE TABLE partsupp( PS_PARTKEY int not null, PS_SUPPKEY int not null, PS_AVAILQTY int not null, PS_SUPPLYCOST      float not null, PS_COMMENT varchar not null);");
	//initialize the appender
	Appender appender(con, "partsupp"); 

	//	con.Query("INSERT INTO partsupp SELECT * FROM read_csv(’../../../data/partsupp.csv’, delim = ‘|’, header=True, columns={’PS_PARTKEY’:’INT’, ‘PS_SUPPKEY’:’INT’, ‘PS_AVAILQTY’:’INT’, ‘PS_SUPPLYCOST’:’FLOAT’, ‘PS_COMMENT’:’VARCHAR’})");
	con.Query("CREATE TABLE integers(i INTEGER)");
	con.Query("INSERT INTO integers VALUES (3)");
	con.Query("INSERT INTO integers VALUES (4)");
	auto result = con.Query("SELECT * FROM partsupp limit 10;");
	result->Print();
}
