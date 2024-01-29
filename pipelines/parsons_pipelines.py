from __future__ import annotations
from typing import Callable, List, Optional, Union, Dict, TypeAlias
from abc import ABC, abstractmethod
from enum import Enum
import os
import functools
from parsons import Table
import requests
from pathlib import Path
import shutil

PipeResult = Enum("PipeResult", ["Unit", "Serial", "Parallel"])
StreamKey: TypeAlias = int
StreamsData: TypeAlias = Dict[StreamKey, Table]


class Logger(ABC):
    def __init__(self):
        self.pipe_offset = 0
        self.pipeline_offset = 0

    def setup(self):
        pass

    def next_pipe(self):
        self.pipe_offset += 1

    def next_pipeline(self):
        self.pipe_offset = 0
        self.pipeline_offset += 1

    @abstractmethod
    def log_data(self, data: Union[Table, StreamsData], pipe_name: str):
        pass


class CsvLogger(Logger):
    def __init__(self, dirname: str = "logging"):
        super().__init__()
        self.dirname = dirname

    def setup(self):
        if not os.path.exists(self.dirname):
            os.makedirs(self.dirname)

    def log_data(self, data: Table | StreamsData, pipe_name: str):
        if isinstance(data, Table):
            filename = f"{self.pipeline_offset}_{self.pipe_offset}_{pipe_name}.csv"
            data.to_csv(os.path.join(self.dirname, filename))
        else:
            for i in data.keys():
                self.log_data(data[i], f"{pipe_name}__stream_{i}")


class BreakpointLogger(Logger):
    def log_data(self, data: Table | StreamsData, pipe_name: str):
        breakpoint()


class Loggers(Logger):
    def __init__(self, *loggers: Logger):
        super().__init__()
        self.loggers = loggers

    def setup(self):
        super().setup()
        for logger in self.loggers:
            logger.setup()

    def next_pipe(self):
        super().next_pipe()
        for logger in self.loggers:
            logger.next_pipe()

    def next_pipeline(self):
        super().next_pipeline()
        for logger in self.loggers:
            logger.next_pipeline()

    def log_data(self, data: Table | StreamsData, pipe_name: str):
        for logger in self.loggers:
            logger.log_data(data, pipe_name)


class PipeBuilder:
    name: str
    func: Callable
    first_pipe: PipeBuilder
    next_pipe: Optional[PipeBuilder]
    input_type: PipeResult
    result_type: PipeResult

    def __init__(self, name, input_type, result_type, func: Callable) -> None:
        self.name = name
        self.func = func
        self.first_pipe = self
        self.next_pipe = None
        self.input_type = input_type
        self.result_type = result_type

    def __repr__(self) -> str:
        return f"{self.name} - {super().__repr__()}"

    def clean_copy(self) -> PipeBuilder:
        """Create a clean copy of this PipeBuilder."""
        return PipeBuilder(self.name, self.input_type, self.result_type, self.func)

    def is_connected(self) -> bool:
        return self.first_pipe != self or self.next_pipe is not None

    def __call__(self, next: PipeBuilder) -> PipeBuilder:
        if self.next_pipe is not None:
            raise RuntimeError(
                "Cannot chain from a pipe that has already been connected."
            )
        if self.result_type != next.input_type:
            raise RuntimeError(
                f"Cannot chain a {self.result_type} output pipe [{self.name}]"
                f" into a {next.input_type} input pipe [{next.name}]."
            )

        if not next.is_connected():
            next.first_pipe = self.first_pipe
            self.next_pipe = next
            return next
        else:
            pipe = next.first_pipe
            self.next_pipe = pipe
            pipe.first_pipe = self.first_pipe
            while pipe.next_pipe is not None:
                pipe = pipe.next_pipe
                pipe.first_pipe = self.first_pipe
            return pipe


class Pipeline:
    def __init__(self, final_pipe: PipeBuilder):
        self.final_pipe = final_pipe

    def run(self, logger: Optional[Logger] = None) -> Union[Table, StreamsData]:
        data: Optional[Union[Table, StreamsData]] = None
        pipe: Optional[PipeBuilder] = self.final_pipe.first_pipe

        if not pipe or pipe.input_type != PipeResult.Unit:
            raise RuntimeError(
                "Must start Pipeline with a source pipeline. (Unit input type.)"
            )

        while pipe is not None:
            # TODO: Check the input type with the data type and throw errors if mismatch
            if pipe.input_type == PipeResult.Unit:
                data = pipe.func()
            else:
                data = pipe.func(data)

            if logger and data:
                logger.log_data(data, pipe.name)
                logger.next_pipe()

            pipe = pipe.next_pipe

        if data is None:
            raise RuntimeError("Lost data somehow. THIS SHOULD NEVER HAPPEN.")

        return data

    def __str__(self) -> str:
        result = ""
        pipe = self.final_pipe.first_pipe
        while pipe:
            result += f"- {pipe.name}\n"
            pipe = pipe.next_pipe
        return result


class Dashboard:
    def __init__(self, *pipelines: Pipeline, logger: Optional[Logger] = None):
        self.pipelines = pipelines
        self.logger = logger

    def run(self):
        if self.logger:
            self.logger.setup()

        for p in self.pipelines:
            p.run(self.logger)

            if self.logger:
                self.logger.next_pipeline()

    def generate_report(self, filename: str):
        html_report = "<html><head><style>"
        html_report += "table {width: 100%; border-collapse: collapse;}"
        html_report += (
            "th, td {border: 1px solid black; text-align: center; padding: 10px;}"
        )
        html_report += "</style></head><body>"
        html_report += "<h2>Pipeline Report</h2>"
        html_report += "<table>"

        for pipeline in self.pipelines:
            html_report += "<tr>"
            pipe = pipeline.final_pipe.first_pipe
            while pipe:
                html_report += f"<td>{pipe.name}</td>"
                pipe = pipe.next_pipe
            html_report += "</tr>"

        html_report += "</table></body></html>"

        with open(filename, "w") as file:
            file.write(html_report)


def define_pipe(
    name: str,
    input_type: PipeResult = PipeResult.Serial,
    result_type: PipeResult = PipeResult.Serial,
):
    def decorator(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> PipeBuilder:
            if input_type == PipeResult.Unit:
                pipe_func = lambda: func(*args, **kwargs)
            else:
                pipe_func = lambda data: func(data, *args, **kwargs)
            return PipeBuilder(name, input_type, result_type, pipe_func)

        return wrapper

    return decorator


@define_pipe("load_from_csv", input_type=PipeResult.Unit)
def load_from_csv(filename: str, **kwargs) -> Table:
    return Table.from_csv(filename, **kwargs)


@define_pipe("load_lotr_books", input_type=PipeResult.Unit)
def load_lotr_books_from_api() -> Table:
    # Set up the endpoint and headers
    url = "https://the-one-api.dev/v2/book"
    headers = {}

    # Make the request to the API
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raises an HTTPError if the response was an error

    # Convert the JSON response into a Parsons Table
    books_json = response.json().get("docs", [])
    books_table = Table(books_json)

    return books_table


@define_pipe("convert")
def convert(data: Table, *args, **kwargs) -> Table:
    results = data.convert_column(*args, **kwargs)
    return results


@define_pipe("write_csv")
def write_csv(data: Table, csv_name: str) -> Table:
    data.to_csv(csv_name)
    return data


@define_pipe("filter_rows")
def filter_rows(data: Table, filters: Union[Callable, str, dict]) -> Table:
    if isinstance(filters, str) or isinstance(filters, Callable):
        results = data.select_rows(filters)
        return results
    else:

        def __select(row: dict) -> bool:
            for k, v in filters.items():
                if isinstance(v, Callable):
                    if not v(row[k]):
                        return False
                else:
                    if row[k] != v:
                        return False
            return True

        results = data.select_rows(__select)
        return results


@define_pipe("split_data", result_type=PipeResult.Parallel)
def split_data(data: Table, bucketing_func: Callable[[dict], StreamKey]) -> StreamsData:
    # TODO: Make this a unique column name
    index_col = "bucket_index"
    data.add_column(index_col, bucketing_func)
    data.materialize()
    # TODO: Make this not the least efficient thing I've ever seen
    results = {}
    for i in set(data[index_col]):  # type: ignore
        results[i] = data.select_rows(lambda r: r[index_col] == i).remove_column(
            index_col
        )
        results[i].materialize()
    return results


@define_pipe("copy_data_into_streams", result_type=PipeResult.Parallel)
def copy_data_into_streams(data: Table, *streams: StreamKey) -> StreamsData:
    streams_data = {}
    for i in streams:
        streams_data[i] = Table(data.to_petl())
    return streams_data


@define_pipe("print_data")
def print_data(data: Table, greeting: str) -> Table:
    print(greeting)
    print("---")
    print(data)
    return data


@define_pipe(
    "all_streams", input_type=PipeResult.Parallel, result_type=PipeResult.Parallel
)
def all_streams(data: StreamsData, inner_pipe: PipeBuilder) -> StreamsData:
    if (
        inner_pipe.input_type != PipeResult.Serial
        or inner_pipe.input_type != PipeResult.Serial
    ):
        raise RuntimeError("Cannot run a non serial-to-serial pipe on all streams")

    for i in data.keys():
        data[i] = inner_pipe.func(data[i])

    return data


# TODO: Refactor the streams concept to return PipeBuilders that contain other
# PipeBuilders, instead of running control flow themselves. This will allow the
# control flow to be handled by the Pipeline object instead.
@define_pipe(
    "for_streams", input_type=PipeResult.Parallel, result_type=PipeResult.Parallel
)
def for_streams(
    data: StreamsData, inner_pipes: Dict[StreamKey, PipeBuilder]
) -> StreamsData:
    for i in data.keys():
        if i in inner_pipes.keys():
            inner_pipe = inner_pipes[i].first_pipe

            while inner_pipe is not None:
                if (
                    inner_pipe.input_type != PipeResult.Serial
                    or inner_pipe.input_type != PipeResult.Serial
                ):
                    raise RuntimeError(
                        "Cannot run a non serial-to-serial pipe on all streams"
                    )

                data[i] = inner_pipe.func(data[i])
                inner_pipe = inner_pipe.next_pipe

    return data


def main():
    for csv in [
        "after_1975.csv",
        "before_1980.csv",
        "after_1979.csv",
        "after_1980.csv",
        "after_1990.csv",
        "lotr_books.csv",
    ]:
        if os.path.exists(csv):
            os.remove(csv)

    dirpath = Path("logging")
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)

    # fmt: off
    clean_year = lambda: (
        filter_rows({
            "Year": lambda year: year is not None
        })
    )(
        convert(
            "Year",
            lambda year_str: int(year_str)
        )
    )

    load_after_1975 = Pipeline(
        load_from_csv("deniro.csv")
        (
            print_data("Raw Data")
        )(
            clean_year()
        )(
            filter_rows({
                "Year": lambda year: year > 1975
            })
        )(
            print_data("After 1975")
        )(
            write_csv("after_1975.csv")
        )
    )
    split_on_1980 = Pipeline(
        load_from_csv("deniro.csv")
        (
            print_data("Raw Data")
        )(
            clean_year()
        )(
            split_data(lambda row: "gte_1980" if row["Year"] >= 1980 else "lt_1980")
        )(
            all_streams(print_data("Split Data"))
        )(
            for_streams({
                "lt_1980": write_csv("before_1980.csv"),
                "gte_1980": write_csv("after_1979.csv")
            })
        )
    )

    save_lotr_books = Pipeline(
        load_lotr_books_from_api()
        (
            write_csv("lotr_books.csv")
        )
    )

    copy_into_streams_test = Pipeline(
        load_from_csv("deniro.csv")
        (
            clean_year()
        )(
            copy_data_into_streams("0", "1")
        )(
            for_streams({
                "0": filter_rows({
                    "Year": lambda year: year > 1990
                })(
                    write_csv("after_1990.csv")
                ),
                "1": write_csv("all_years.csv")
            })
        )
    )

    dashboard = Dashboard(
        split_on_1980,
        save_lotr_books,
        load_after_1975,
        copy_into_streams_test,
        logger=Loggers(
            CsvLogger(),
            # BreakpointLogger()
        )
    )
    # dashboard.run()
    # dashboard.generate_report("report.html")
    # fmt: on

    load_after_1975_dask()


import dask.dataframe as dd


def load_after_1975_dask():
    # Load data
    data = dd.read_csv("deniro.csv", quotechar='"')

    # Print raw data
    print("Raw Data")
    print(data.head(10))

    # Clean year - remove None and convert to int
    data = data[data["Year"].notnull()]
    data["Year"] = data["Year"].astype(int)

    # Filter rows where Year > 1975
    data = data[data["Year"] > 1975]

    # Print data after filtering
    print("After 1975")
    print(data.head(10))

    # Write to CSV
    data.to_csv("after_1975.csv", single_file=True)


if __name__ == "__main__":
    main()