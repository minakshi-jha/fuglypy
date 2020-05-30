import gzip
import io
import re

import boto3
import pandas as pd

try:
    s3_client = boto3.client('s3'
                             , aws_access_key_id="aws_access_key_id"
                             , aws_secret_access_key="aws_secret_access_key")
    emr_client = boto3.client('emr'
                              , aws_access_key_id="aws_access_key_id"
                              , aws_secret_access_key="aws_secret_access_key")

    page_iterator = emr_client.get_paginator('list_clusters').paginate()
    clusters = list()
    # clusters.append("")
    for page in page_iterator:
        for item in page['Clusters']:
            clusters.append(item['Id'])

    log_list = []
    cluster_count = 0
    print("Cluster count to be processed = {0}".format(len(clusters)))
    for cluster in clusters:
        cluster_count = cluster_count + 1
        print("Cluster Sequence = {0}".format(cluster_count))
        s3 = boto3.client('s3')
        log_dict = {}
        print(cluster)
        steps = s3_client.list_objects_v2(Bucket="Bucket", Prefix="logs/"+cluster+"/steps/")
        if "Contents" in steps.keys():
            for step in steps['Contents']:
                # if "stderr" in obj['Key'] and "s-XXXXXXXXXXXXX" in obj['Key']:
                if "stderr" in step['Key']:
                    response = s3_client.get_object(Bucket="Bucket", Key=step['Key'])
                    content = response['Body'].read()
                    with gzip.GzipFile(fileobj=io.BytesIO(content), mode='rb') as fh:
                        content = fh.read().decode("utf-8")
                        if "Error" in content:
                            error_msg = ""
                            for line in content.split("\n"):
                                if "Error" in line or "exception" in line:
                                    error_msg = error_msg + line

                            cluster_id = re.search('j-[a-zA-Z0-9]+', step['Key']).group(0)
                            step_id = re.search('s-[a-zA-Z0-9]+', step['Key']).group(0)

                            cluster_name = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['Name']
                            step_name = emr_client.list_steps(ClusterId=cluster_id, StepIds=[step_id])['Steps'][0]['Name']

                            log_dict["log_date"] = step["LastModified"].strftime("%Y-%m-%d %H:%M:%S")
                            log_dict["cluster_id"] = cluster_id
                            log_dict["cluster_name"] = cluster_name
                            log_dict["step_id"] = step_id
                            log_dict["step_name"] = step_name
                            log_dict["error_message"] = error_msg
                            log_list.append(log_dict)
                            print(log_dict)
    print(log_list)
    df = pd.DataFrame(log_list)
    df.to_csv("emr_error_log.csv", index=False, header=True)

except Exception as ex:
        print(ex)
