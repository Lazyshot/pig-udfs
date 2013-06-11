register $JAR_PATH

define PROD PRODUCT();

data_in = LOAD 'input' as (val:double);
data_out = GROUP data_in ALL;
data_out = FOREACH data_out GENERATE PROD(data_in.val) AS product; 

/*describe data_out;*/
STORE data_out into 'output';