#include "duckdb.hpp"
#include <iostream>
#include <memory>
#include <vector>
#include <string>

using namespace duckdb;

int main() {
    try {
        // Create a new database in in-memory mode
        DuckDB db(nullptr);
        Connection con(db);

        // Create a more complex schema with multiple data types
        con.Query(R"(
            CREATE TABLE employees(
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                age INTEGER,
                salary DECIMAL(10,2),
                hire_date DATE,
                is_active BOOLEAN
            )
        )");

        // Insert multiple records using prepared statements for better performance
        auto insert = con.Prepare("INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?)");

        insert->Execute(1, "Alice Johnson", 30, 75000.00, Date::FromDate(2020, 5, 15), true);
        insert->Execute(2, "Bob Smith", 45, 85000.00, Date::FromDate(2018, 3, 22), true);
        insert->Execute(3, "Carol Davis", 28, 65000.00, Date::FromDate(2022, 11, 3), true);
        insert->Execute(4, "David Wilson", 52, 95000.00, Date::FromDate(2015, 7, 10), false);
        insert->Execute(5, "Eve Brown", 35, 70000.00, Date::FromDate(2021, 1, 20), true);

        // Demonstrate different query types
        std::cout << "=== All employees ===" << std::endl;
        auto all_employees_result = con.Query(R"(
            SELECT *
            FROM employees
            ORDER BY id
        )");
        all_employees_result->Print();

        std::cout << "\n=== Active employees with salary > 70000 ===" << std::endl;
        auto active_result = con.Query(R"(
            SELECT name, salary
            FROM employees
            WHERE is_active = true AND salary > 70000
            ORDER BY salary DESC
        )");
        active_result->Print();

        // Demonstrate prepared statements for queries
        std::cout << "\n=== Employee details for ID = 2 ===" << std::endl;
        auto prepared_query = con.Prepare(R"(
            SELECT name, age, salary
            FROM employees
            WHERE id = ?
        )");
        auto query_result = prepared_query->Execute(2);
        query_result->Print();

    } catch (const std::exception &ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}