# fuglypy
######Its fugly but beautiful


#####emr_log_utility: 
    In some cases old s3 logs and clusters instances may not be available.
    So, get all available cluster id from emr first using paginator. Now for every cluster get all step zipped log from s3.
    If step has any error, get cluster and step name using cluster id and step id. Get s3 file modified date as log date.
    Fetch lines from s3 content which contains "error" or "exception". Create dictionary for step, list for cluster.
    Convert list of dictionary to dataframe and save to csv.
    Feature enhancement: have another column for error code from error message line which contains "return code 1 from"
    
    Input = aws_access_key_id, aws_secret_access_key, Bucket name, emr s3 log path
    Output = csv with columns log_date, cluster_id, cluster_name, step_id, step_name, error_message
    Python package used = gzip, io, re, boto3, pandas
 
 
#####upcoming:
    .....