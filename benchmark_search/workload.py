# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

# This code needs to be included at the top of every workload.py file.
# OpenSearch Benchmarks is not able to find other helper files unless the path
# is updated.
import os
import sys
import copy
import json


class ConfigurationError(Exception):
    """Exception raised for errors configuration.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message: str):
        self.message = f"{message}"
        super().__init__(self.message)


def parse_string_parameter(key: str, params: dict, default: str = None) -> str:
    if key not in params:
        if default is not None:
            return default
        raise ConfigurationError("Value cannot be None for param {}".format(key))

    if type(params[key]) is str:
        return params[key]

    raise ConfigurationError("Value must be a string for param {}".format(key))


def parse_int_parameter(key: str, params: dict, default: int = None) -> int:
    if key not in params:
        if default:
            return default
        raise ConfigurationError("Value cannot be None for param {}".format(key))

    if type(params[key]) is int:
        return params[key]

    raise ConfigurationError("Value must be a int for param {}".format(key))


def parse_list_parameter(key: str, params: dict, default=None):

    if default is None:
        default = []
    if key not in params:
        if default:
            return default
        raise ConfigurationError("Value cannot be None for param {}".format(key))

    if type(params[key]) is list:
        return params[key]

    raise ConfigurationError("Value must be a list for param {}".format(key))


sys.path.append(os.path.abspath(os.getcwd()))


# This is the entry point for the workloads
def register(registry):
    register_workload_extensions(registry)


def register_workload_extensions(registry):
    registry.register_param_source(
        "neural-search-query-params-source", QueryNeuralSearchParamsSource
    )


class QueryNeuralSearchParamsSource:
    """
    A `ParamSource` captures the parameters for a given operation.
     Benchmark will create one global ParamSource for each operation and will then
     invoke `#partition()` to get a `ParamSource` instance for each client. During the benchmark, `#params()` will be called repeatedly
     before Benchmark invokes the corresponding runner (that will actually execute the operation against OpenSearch).
    """

    def __init__(self, workload, params, **kwargs):

        self.index_name: str = parse_string_parameter("index", params)
        self.fields_to_be_excluded_from_source = parse_list_parameter(
            "fields_to_excluded", params
        )
        self.query_data_set_path: str = parse_string_parameter("data_set_path", params)
        self.model_id = parse_string_parameter("model_id", params)
        self.method = parse_string_parameter("method", params)
        self.query_data_file = QueryDataSet(self.query_data_set_path)

        # total number of queries in the file
        self.total_queries = self.query_data_file.total_lines
        self.infinite = False
        self.percent_completed = 0

        self.offset = 0
        self.current = 0
        self.queries_per_client = 0
        self.query_count_of_client = 0

    """
    partition_index : client which is getting hit
    total_partitions : total number of client represented as clients
    """

    def partition(self, partition_index, total_partitions):
        """
        This method will be invoked by Benchmark at the beginning of the lifecycle. It splits a parameter source per client. If the
        corresponding operation is idempotent, return `self` (e.g. for queries). If the corresponding operation has side-effects and it
        matters which client executes which part (e.g. an index operation from a source file), return the relevant part.

        Do NOT assume that you can share state between ParamSource objects in different partitions (technical explanation: each client
        will be a dedicated process, so each object of a `ParamSource` lives in its own process and hence cannot share state with other
        instances).

        :param partition_index: The current partition for which a parameter source is needed. It is in the range [0, `total_partitions`).
        :param total_partitions: The total number of partitions (i.e. clients).
        :return: A parameter source for the current partition.
        """
        print(
            "******* Index Name : {} partition_index {} , total_parations : {}\n".format(
                self.index_name, partition_index, total_partitions
            )
        )
        # return the copy of params source

        partition_x = copy.copy(self)

        partition_x.queries_per_client = self.total_queries // total_partitions
        partition_x.offset = (partition_index * partition_x.queries_per_client) + 1
        partition_x.current = partition_index

        return partition_x

    # This will be called per client
    def params(self):

        if self.query_count_of_client >= self.queries_per_client:
            # print("Stopping iteration for client {} ".format(self.current))
            # raise StopIteration
            self.query_count_of_client = 0

        query_raw = json.loads(
            self.query_data_file.read_line(
                self.offset + self.query_count_of_client
            ).strip()
        )
        query = {
            "query": {
                "neural_sparse": {
                    "text_sparse": {
                        "query_tokens": query_raw["sparse_embedding"],
                    }
                }
            }
        }
        self.query_count_of_client += 1
        self.percent_completed = self.query_count_of_client / self.queries_per_client
        q = {
            "index": self.index_name,
            "request-params": {"_source": False},
            "body": query,
        }

        return q


class QueryDataSet:
    def __init__(self, file_name):
        self.file_name = file_name
        self.lines = []
        self.total_lines = self.count_lines()

    def count_lines(self):
        count = 0
        with open(self.file_name) as file:
            for line in file:
                self.lines.append(line)
                count = count + 1
            file.close()
        # print("Lines count is {}".format(count))
        return count

    def read_line(self, line_number):
        if line_number > self.total_lines or line_number < 0:
            raise Exception(
                "Line number provided is not valid. Line Number: {} ".format(
                    line_number
                )
            )
        # print("\n Line number is : {}\n".format(line_number))
        return self.lines[int(line_number) - 1]
