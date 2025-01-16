CREATE EXTERNAL TABLE datarag_assets
LOCATION '{datarag_path}/datarag_assets'
TBLPROPERTIES (
  'table_type'='DELTA'
);

select * from datarag_assets;

CREATE EXTERNAL TABLE datarag_results
LOCATION '{datarag_path}/datarag_results'
TBLPROPERTIES (
  'table_type'='DELTA'
);

select * from datarag_results order by timestamp desc;
