CREATE FUNCTION Insert_card(bigint, numeric, numeric, boolean, smallint, character varying(64)) RETURNS VOID 
AS ' 
 INSERT INTO card VALUES ($1, $2, $3, $4, $5, $6);
'LANGUAGE sql;
