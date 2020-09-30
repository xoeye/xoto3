"""Many AWS services send JSON objects with an array of records to
asynchronous processors such as Lambdas.

In many cases we wish to process these records invididually, and let
individual records 'fail' while noting that some of the records may
have been successfully processed.

The cleanest way to do this is via an abstraction that feeds each
invididual record from an array into a provided callable that accepts
a single record of arbitrary type and either runs successfully or
raises an Exception. The exceptions are individually caught, and then
the records that failed may be collected along the way and a wrapper
exception thrown for the group. This may then be processed by
application-level code, or may be wrapped by some other library
primitive like the Dead Letter service.
"""
import typing as ty

from typing_extensions import Protocol, TypedDict

from .types import Event, LambdaContext, LambdaEntryPoint, LambdaEntryPointDecorator

# types

Record = ty.Dict[str, ty.Any]
RecordProcessor = ty.Callable[[Record, int], ty.Any]
RecordContainer = TypedDict("RecordContainer", {"Records": ty.List[Record]})
RecordFailure = TypedDict(
    "RecordFailure",
    {"record_index": int, "exception_name": str, "exception_str": str, "exception": Exception,},
)


def format_failed_records(failed_records: ty.List[RecordFailure]):
    if len(failed_records) == 1:
        # common case; for readability
        record = failed_records[0]
        return (
            f"{record['exception_name']} :: {record['exception_str']} :: {record['record_index']}"
        )
    exception_names = list(set(record["exception_name"] for record in failed_records))
    if len(exception_names) == 1:
        # less common case but also helpful for readability
        return f"{len(failed_records)} {exception_names[0]}"
    return f"{len(failed_records)} failed records"


class FailedRecordsException(Exception):
    def __init__(self, failed_records: ty.List[RecordFailure]):
        self.failed_records = failed_records

    def __str__(self):
        return format_failed_records(self.failed_records)


# implementation


def process_records_yield_failures(
    record_processor: RecordProcessor, records: ty.Sequence[Record]
) -> ty.Iterable[RecordFailure]:
    for i, record in enumerate(records):
        try:
            record_processor(record, i)
        except Exception as e:
            record_failure: RecordFailure = dict(
                # record=record,  # this will make some dead letters too large
                record_index=i,
                exception_name=str(e.__class__.__name__),
                exception_str=str(e),
                exception=e,
            )
            yield record_failure


class StreamProcessor(Protocol):
    """A stream processor receives a portion of a Record from an Event
    but also receives the entire event and context if it is interested in them.

    If a specific record within the stream causes an Exception to be
    raised, that exception will be caught, and that specific record will
    be noted as a failure in whatever gets sent to a wrapping dead letter
    service.

    In general the return value, if any, is thrown away.

    """

    def __call__(
        self,
        __transformed_record: ty.Any,
        *,
        records: ty.List[Record],
        record_index: int,
        event: Event,
        context: LambdaContext,
    ) -> ty.Any:
        ...


StreamProcessorDecorator = ty.Callable[[StreamProcessor], LambdaEntryPoint]
RecordTransformer = ty.Callable[[Record], ty.Any]
RecordFilter = ty.Callable[[Record], bool]  # if false, do not process record


def make_decorated_individual_stream_processor_decorator(
    lambda_entry_point_decorator: LambdaEntryPointDecorator,
    record_prep_transformer: RecordTransformer,
    record_filter: RecordFilter = lambda _r: True,
) -> StreamProcessorDecorator:
    """The filter runs *before* the prep_transformer"""

    def make_decorated_stream_processor(processor: StreamProcessor) -> LambdaEntryPoint:
        @lambda_entry_point_decorator
        def transform_all_records(event: Event, context: LambdaContext):
            # allow stream processors to drop records however they need to
            records = event["Records"]

            def record_processor(record: Record, record_index: int):
                if record_filter(record):
                    processor(
                        record_prep_transformer(record),
                        records=records,
                        record_index=record_index,
                        event=event,
                        context=context,
                    )

            failures = list(process_records_yield_failures(record_processor, records))
            if failures:
                raise FailedRecordsException(failures)

        return transform_all_records

    return make_decorated_stream_processor
