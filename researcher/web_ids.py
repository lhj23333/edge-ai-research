from typing import Annotated

from pydantic import BeforeValidator, Field, TypeAdapter, ValidationError


SQLITE_MAX_ID = 2**63 - 1
RecordId = Annotated[int, Field(ge=1, le=SQLITE_MAX_ID)]
OptionalRecordId = Annotated[RecordId | None, BeforeValidator(lambda value: None if value == "" else value)]
record_id_adapter = TypeAdapter(RecordId)


def parse_record_id(value):
    try:
        return record_id_adapter.validate_python(value)
    except ValidationError as error:
        raise ValueError(f"记录编号必须是 1 至 {SQLITE_MAX_ID} 之间的整数") from error
