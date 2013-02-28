CREATE FUNCTION insert_country_specific(VARCHAR(2), BOOLEAN, DECIMAL) RETURNS VOID 
AS ' 
 INSERT INTO country_specific VALUES ($1, $2, $3);
'LANGUAGE sql;
