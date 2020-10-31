Main idea:
Cloud Sheduler periodicly run POST REST load command on Spring Boot app runnnig at Cloud Run. 
Spring checks Cloud Storage for files with .avro extension, precess them to two self explained tables(avro_all and avro_non_optionl).
At first all data to avro_all, then get schema from this table and filter it for non optional (REQUIRED) fileds, and add required data to avro_non_optional table;
Then processed files been delete from Cloud Storage and wait for new files.
