#!/usr/bin/env python3

import logging
import argparse
import pandas as pd
import boto3
from tabulate import tabulate

logger = logging.getLogger()
logging.basicConfig()
logger.setLevel(logging.INFO)

# create the top-level parser
parser = argparse.ArgumentParser()

# create root-parser for commands
sub_parsers = parser.add_subparsers(dest="command", help='sub-command help')

# sqs
CMD_SQS: str = "sqs"
parser_sqs = sub_parsers.add_parser(name=CMD_SQS, help='Operations for interacting with AWS SQS')
parser_sqs_command = parser_sqs.add_subparsers(dest="operation", help='sub-sub-command help')

CMD_SQS_CHECK: str = "check"
print(CMD_SQS_CHECK)
parser_sqs_command_check = parser_sqs_command.add_parser(name=CMD_SQS_CHECK, help='it is crazy sub-sub-command')
parser_sqs_command_check.add_argument('-n', '--name', required=False, help='Name pattern of the queue')
parser_sqs_command_check.add_argument('-c', '--columns', nargs='+', required=False, help="Columns to be displayed",
                                      default=["name", "url", "arn", "count", "in-flight", "msg-delayed"])
parser_sqs_command_check.add_argument('-p', '--processing', help="Show only queues which have messages",
                                      action='store_true')

# sqs -> normal
parser_sqs_command_normal = parser_sqs_command.add_parser('normal', help='it is normal sub-sub-command')
parser_sqs_command_normal.add_argument('--common', action='store_const', const=True, help='it is common option')
#
# # create the parser for the "booo" sub-command
# parser_booo = sub_parsers.add_parser('booo', help='booo is also cool sub-command')
# parser_booo.add_argument('--baz', choices='XYZ', help='baz is another option')
# parser_booo.add_argument('--zaz', action='store_const', const=True, help='zaz is French singer')

args = parser.parse_args()

logging.info(args)

if args.command == CMD_SQS:
    sqs = boto3.resource('sqs')
    sqs_client = boto3.client('sqs')

    if args.operation == CMD_SQS_CHECK:
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
            result_data.setdefault("in-flight", []).append(
                queue_attributes["Attributes"]["ApproximateNumberOfMessagesNotVisible"])
            result_data.setdefault("msg-delayed", []).append(
                queue_attributes["Attributes"]["ApproximateNumberOfMessagesDelayed"])

        logger.info(result_data)
        output = pd.DataFrame(data=result_data)

        if args.processing:
            output = output[(output["count"] != "0")]

        columns = ["name"] + args.columns + ["count", "in-flight", "msg-delayed"]
        # columns.append("arn")

        if not output.empty:
            print(tabulate(output[list(dict.fromkeys(columns))], headers='keys', tablefmt='psql'))
        else
            print("There are not queues")

    else:
        raise Exception("incorrect command.")
