from primitive.nftime import utc
from primitive.signature import S
from primitive.utils import index


class BaseDataType:
    def get_db_type_name(self):
        return self.db_type


class Date(BaseDataType):
    db_type = "date"

    def code_to_db(self, value):
        return f"'{value.isoformat()}'"


class DateTime(BaseDataType):
    db_type = "timestamp with time zone"

    def code_to_db(self, value):
        return f"'{value.astimezone(utc).isoformat()}'"


class Text(BaseDataType):
    db_type = "text"

    def code_to_db(self, value):
        escaped_value = value.replace("'", "''")
        return f"'{escaped_value}'"


class Int(BaseDataType):
    db_type = "integer"

    def code_to_db(self, value):
        return f"{value}"


class UUID(BaseDataType):
    db_type = "uuid"

    def code_to_db(self, value):
        return f"'{value}'"


@S.init(precision=S.p, scale=S.p)
class Numeric(BaseDataType):
    def get_db_type_name(self):
        return f"NUMERIC({self.precision}, {self.scale})"

    def code_to_db(self, value):
        return f"'{value}'"


class Boolean(BaseDataType):
    db_type = "boolean"


def parse_type(text_type):
    text_type = text_type.lower()
    if text_type.startswith("numeric"):
        param_str = text_type[len("numeric") :]
        assert param_str[0] == "("
        assert param_str[-1] == ")"
        params = param_str[1:-1].split(",")
        return Numeric(int(params[0].strip()), int(params[1].strip()))
    else:
        type_name_to_cls = index(
            [Date, DateTime, Text, Int, UUID, Boolean], X().get_db_type_name().lower()
        )
        return type_name_to_cls[text_type]()
