This is a self understanding project involving below tools

1) AWS ( S3, IAM )
2) Python ( pandas,snowflake,json )
3) Snowflake ( JSON flattening ) 

**Overview:**  Data of the restaurants and their ratings all over india registered in swiggy is being stored in a S3 bucket and the same is now 
being extracted from S3 via python and some basic transformation like  replacig null values, filtering over the  south india region and sending these data as json records to the snowflake stg table
later in snowflake these json records are being flattened for further storage.

