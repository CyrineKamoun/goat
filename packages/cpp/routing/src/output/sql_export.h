#pragma once

#include <string>

namespace duckdb
{
    class Connection;
}

namespace routing::output
{

// Escape single quotes so a string can be embedded in a SQL string literal.
std::string sql_escape(std::string const &s);

// Write the result of a COPY body to a Parquet file using the project-standard
// options (FORMAT PARQUET, COMPRESSION ZSTD). Creates the parent directory and
// escapes the path. `copy_body` is whatever goes inside `COPY ( ... )` — a
// SELECT statement or a bare table name. On failure, throws std::runtime_error
// prefixed with `error_label`.
void write_query_to_parquet(duckdb::Connection &con,
                            std::string const &copy_body,
                            std::string const &output_path,
                            std::string const &error_label);

} // namespace routing::output
