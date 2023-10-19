duckdb -c "select count(*) from output.parquet" 

duckdb -c "DESCRIBE select * from output.parquet" 
