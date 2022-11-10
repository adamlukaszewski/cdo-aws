#!/usr/bin/env python3
import logging

import boto3
import pandas as pd
import argparse

from tabulate import tabulate

logger = logging.getLogger()
logging.basicConfig()
logger.setLevel(logging.INFO)

parser = argparse.ArgumentParser(description="Prints information about the queues")
parser.add_argument('-n', '--name', required=False, help='Name pattern of the queue')
parser.add_argument('-c', '--columns', nargs='+', required=False, help="Columns to be displayed", default=["name", "url", "arn", "count", "in-flight", "msg-delayed"])
parser.add_argument('-p', '--processing', help="Show only queues which have messages",  action='store_true')
args = parser.parse_args()

sqs = boto3.resource('sqs')
sqs_client = boto3.client('sqs')

all_queues = sqs.queues.all()
queues_to_parse = [{"name": queue.url.rsplit('/', 1)[-1], "url": queue.url} for queue in all_queues]
if args.name:
    queues_to_parse = list(filter(lambda x: args.name in x['name'], queues_to_parse))
    # queues_to_parse = list(filter(lambda x: args.name in x, queues_to_parse))

result_data = {}
for queue_name in queues_to_parse:
    queue_attributes = sqs_client.get_queue_attributes(QueueUrl=queue_name["url"],
                                                       AttributeNames=["QueueArn",
                                                                       "ApproximateNumberOfMessages",
                                                                       "ApproximateNumberOfMessagesNotVisible",
                                                                       "ApproximateNumberOfMessagesDelayed"])

    result_data.setdefault("name", []).append(queue_name["name"])
    result_data.setdefault("url", []).append(queue_name["url"])
    result_data.setdefault("arn", []).append(queue_attributes["Attributes"]["QueueArn"])
    result_data.setdefault("count", []).append(queue_attributes["Attributes"]["ApproximateNumberOfMessages"])
    result_data.setdefault("in-flight", []).append(queue_attributes["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
    result_data.setdefault("msg-delayed", []).append(queue_attributes["Attributes"]["ApproximateNumberOfMessagesDelayed"])

# logger.info(result_data)
# logger.info(result_info)
output = pd.DataFrame(data=result_data)

if args.processing:
    output = output[(output["count"] != "0")]

columns = ["name"] + args.columns + ["count", "in-flight", "msg-delayed"]
# columns.append("arn")

print(tabulate(output[list(dict.fromkeys(columns))], headers='keys', tablefmt='psql'))
