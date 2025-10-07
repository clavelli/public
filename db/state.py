import uuid

from munch import Munch

from infra.db import settings
from infra.db.connection import get_connection
from infra.db.data_type import UUID, Int, Text, parse_type
from infra.db.field import ForeignKeyField
from infra.shape import F
from primitive.utils import S, get_opt, index


@S.init(name=S.p, id=S.a(None), columns=S.v([]))
@S.repr(name=S.r)
class Table:
    def get_column(self, name):
        return get_opt(self.columns, X.name == name)


def serialize_value_for_default(type, value):
    if value is None:
        return "NULL"
    if isinstance(type, Text):
        return f"'{value}'"
    elif isinstance(type, Int):
        return f"'{value}'"
    else:
        assert False


def deserialize_value_for_default(type, value):
    if value is None:
        return None
    assert value != "NULL"  # We should be getting None, not "NULL" I think
    if type == "text":
        assert isinstance(value, str)
        return value
    elif type == "integer":
        return int(value)
    else:
        assert False


@S.init(
    table=S.p,
    name=S.p,
    type=S.p,
    nullable=S.p,
    id=S.a(None),
    fk_constraint=S.a(None),
    is_unique=S.a(False),
    default=S.a(None),
    has_default=S.a(False),
)
@S.repr(name=S.r, type=S.r)
class Column:
    def has_index(self):
        return (self.fk_constraint or self.is_unique) and self.name != "id"

    def index_name(self):
        return (
            "fk" if self.fk_constraint else "idx"
        ) + f"_{self.table.name}_{self.name}"


@S.init(column=S.p, referenced_table=S.p, on_delete=S.a(), id=S.a(None))
class ForeignKeyConstraint:
    pass


def get_operations(code_tables, db_tables):
    name_to_code_table = index(code_tables, X.name)
    name_to_db_table = index(db_tables, X.name)
    names = set(name_to_code_table.keys()) | set(name_to_db_table.keys())
    name_to_tables = {
        name: (name_to_code_table.get(name), name_to_db_table.get(name))
        for name in names
    }
    name_to_new_db_table = {}
    operations = []
    logical_operations = []
    for name, (code_table, db_table) in name_to_tables.items():
        if not db_table:
            operations.append(TableOperation("create", name=code_table.name))
            db_table = Table(code_table.name, id=uuid.uuid4())
            db_column = Column(
                db_table, "id", UUID(), False, is_unique=True, id=uuid.uuid4()
            )
            db_table.columns.append(db_column)
            name_to_new_db_table[name] = db_table
            logical_operations.append(
                f"INSERT INTO migration_management.table (id, name) VALUES ('{db_table.id}', '{db_table.name}')"
            )
            logical_operations.append(
                f"INSERT INTO migration_management.column (id, name, table_id, default_value, has_default, data_type, nullable, is_unique) VALUES ('{db_column.id}', 'id', '{db_table.id}', NULL, false, 'uuid', false, true)"
            )

    for name, (code_table, db_table) in name_to_tables.items():
        if db_table is None:
            db_table = name_to_new_db_table[name]
        if code_table:
            name_to_code_column = index(code_table.columns, X.name)
            name_to_db_column = index(db_table.columns, X.name)
            # set join puts us in a random order, this makes us match the order in the schema which is nice
            names = list(name_to_code_column.keys()) + [
                x for x in name_to_db_column.keys() if x not in name_to_code_column
            ]
            name_to_columns = {
                name: (name_to_code_column.get(name), name_to_db_column.get(name))
                for name in names
            }
            for name, (code_column, db_column) in name_to_columns.items():
                if not db_column:
                    operations.append(
                        ColumnOperation(
                            "create",
                            table_name=code_column.table.name,
                            name=code_column.name,
                            type=code_column.type.get_db_type_name(),
                            nullable=code_column.nullable,
                            default=(
                                code_column.type.code_to_db(code_column.default)
                                if code_column.default is not None
                                else "NULL"
                            ),
                            has_default=code_column.has_default,
                        )
                    )
                    db_column = Column(
                        db_table,
                        code_column.name,
                        code_column.type,
                        code_column.nullable,
                        default=code_column.default,
                        has_default=code_column.has_default,
                        is_unique=False,  # We add uniqueness later so still false at this point
                        id=uuid.uuid4(),
                    )
                    logical_operations.append(
                        f"INSERT INTO migration_management.column (id, name, table_id, data_type, nullable, default_value, has_default, is_unique) VALUES ('{db_column.id}', '{db_column.name}', '{db_column.table.id}', '{db_column.type.get_db_type_name()}', {db_column.nullable}, {serialize_value_for_default(db_column.type, db_column.default)}, {db_column.has_default}, {db_column.is_unique})"
                    )
                if code_column:
                    code_column.id = db_column.id
                    mismatches = {}
                    for field in ["nullable", "has_default", "is_unique"]:
                        if getattr(code_column, field) != getattr(db_column, field):
                            mismatches[field] = getattr(code_column, field)
                    if (
                        code_column.type.get_db_type_name()
                        != db_column.type.get_db_type_name()
                    ):
                        mismatches["type"] = code_column.type.get_db_type_name()
                    if code_column.default != db_column.default:
                        mismatches["default"] = (
                            code_column.type.code_to_db(code_column.default)
                            if code_column.default is not None
                            else "NULL"
                        )
                    if mismatches:
                        update_expressions = []
                        if "is_unique" in mismatches:
                            is_unique = mismatches.pop("is_unique")
                            update_expressions.append(f"is_unique={is_unique}")

                        if mismatches:
                            operations.append(
                                ColumnOperation(
                                    "update",
                                    table_name=code_column.table.name,
                                    name=code_column.name,
                                    **mismatches,
                                )
                            )
                        for k in ["nullable", "has_default"]:
                            if k in mismatches:
                                update_expressions.append(f"{k}={mismatches[k]}")

                        if "type" in mismatches:
                            update_expressions.append(
                                f"data_type='{mismatches['type']}'"
                            )
                        if "default" in mismatches:
                            update_expressions.append(
                                f"default_value={serialize_value_for_default(code_column.type, code_column.default)}"
                            )

                        logical_operations.append(
                            f"UPDATE migration_management.column SET {', '.join(update_expressions)} WHERE id='{db_column.id}'"
                        )

                    if code_column.has_index():
                        should_delete_index = (
                            db_column.has_index()
                            and code_column.is_unique != db_column.is_unique
                        )
                        if should_delete_index:
                            operations.append(
                                IndexOperation(
                                    "delete",
                                    table_name=code_column.table.name,
                                    name=code_column.index_name(),
                                )
                            )
                        if db_column.has_index() and not should_delete_index:
                            if code_column.index_name() != db_column.index_name():
                                operations.append(
                                    IndexOperation(
                                        "rename",
                                        table_name=code_column.table.name,
                                        name=db_column.index_name(),
                                        new_name=code_column.index_name(),
                                    )
                                )
                        else:
                            operations.append(
                                IndexOperation(
                                    "create",
                                    table_name=code_column.table.name,
                                    name=code_column.index_name(),
                                    column_names=[code_column.name],
                                    is_unique=code_column.is_unique,
                                )
                            )
                    else:
                        if db_column.has_index():
                            operations.append(
                                IndexOperation(
                                    "delete",
                                    table_name=code_column.table.name,
                                    name=code_column.index_name(),
                                )
                            )
                    code_constraint = code_column.fk_constraint
                    db_constraint = db_column.fk_constraint
                    if code_constraint and db_constraint:
                        # TODO
                        pass
                    elif code_constraint and not db_constraint:
                        operations.append(
                            ForeignKeyConstraintOperation(
                                "create",
                                table_name=code_constraint.column.table.name,
                                name=f"fk_{code_constraint.column.name}",
                                column_name=code_constraint.column.name,
                                referenced_table_name=code_constraint.referenced_table.name,
                                on_delete=code_constraint.on_delete,
                            )
                        )
                        _, referenced_table = name_to_tables[
                            code_constraint.referenced_table.name
                        ]
                        if referenced_table is None:
                            referenced_table = name_to_new_db_table[
                                code_constraint.referenced_table.name
                            ]
                        logical_operations.append(
                            f"INSERT INTO migration_management.fk_constraint (id, column_id, referenced_table_id, on_delete) VALUES ('{uuid.uuid4()}', '{db_column.id}', '{referenced_table.id}', '{code_constraint.on_delete}')"
                        )
                    elif not code_constraint and db_constraint:
                        operations.append(
                            ForeignKeyConstraintOperation(
                                "delete",
                                table_name=code_table.name,
                                name=f"fk_{db_constraint.column.name}",
                            )
                        )
                        logical_operations.append(
                            f"DELETE FROM migration_management.fk_constraint WHERE id='{db_constraint.id}'"
                        )
                    else:
                        pass
                else:
                    operations.append(
                        ColumnOperation(
                            "delete",
                            table_name=db_column.table.name,
                            name=db_column.name,
                        )
                    )
                    logical_operations.append(
                        f"DELETE FROM migration_management.column WHERE id='{db_column.id}'"
                    )
    for name, (code_table, db_table) in name_to_tables.items():
        if not code_table:
            operations.append(TableOperation("delete", name=db_table.name))
            logical_operations.append(
                f"DELETE FROM migration_management.table WHERE id='{db_table.id}'"
            )
    return operations, logical_operations


def get_db_state():
    conn = get_connection("admin_user")
    cur = conn.execute("SELECT * FROM migration_management.table")
    db_tables = cur.fetchall()
    cur = conn.execute("SELECT * FROM migration_management.column")
    all_db_columns = cur.fetchall()
    cur = conn.execute("SELECT * from migration_management.fk_constraint")
    all_db_fk_constraints = cur.fetchall()
    tables = [Table(x.name, id=x.id) for x in db_tables]
    id_to_table = index(tables, X.id)
    id_to_column = {}
    for db_column in all_db_columns:
        table = id_to_table[db_column.table_id]
        column = Column(
            table,
            db_column.name,
            parse_type(db_column.data_type),
            db_column.nullable,
            default=deserialize_value_for_default(
                db_column.data_type, db_column.default_value
            ),
            has_default=db_column.has_default,
            is_unique=db_column.is_unique,
            id=db_column.id,
        )
        id_to_column[column.id] = column
        table.columns.append(column)

    for db_fk_constraint in all_db_fk_constraints:
        column = id_to_column[db_fk_constraint.column_id]
        fk_constraint = ForeignKeyConstraint(
            column,
            id_to_table[db_fk_constraint.referenced_table_id],
            on_delete=db_fk_constraint.on_delete,
            id=db_fk_constraint.id,
        )
        assert column.fk_constraint is None
        column.fk_constraint = fk_constraint
    return tables


def get_code_state(models):
    tables = []
    for model in models:
        table = Table(model.table_name())
        tables.append(table)
        columns = []
        for name, field in model.get_schema(migration=True).items():
            # TODO would be better to compare their type objects instead of strings
            columns.append(
                Column(
                    table,
                    name,
                    field.get_db_type(),
                    field.null,
                    default=field.db_default,
                    has_default=field.db_default_provided,
                    is_unique=field.unique,
                )
            )
        table.columns = columns
    name_to_table = index(tables, X.name)
    for model in models:
        for name, field in model.get_schema(migration=True).items():
            if isinstance(field, ForeignKeyField):
                table = name_to_table[model.table_name()]
                column = table.get_column(name)
                referenced_table = name_to_table[field.model.table_name()]
                column.fk_constraint = ForeignKeyConstraint(
                    column, referenced_table, on_delete=field.on_delete
                )
    return tables


class BaseOperation:
    def __init__(self, operation_type, **kwargs):
        self.operation_type = operation_type
        self.values = Munch()
        for identifier in self.identifiers:
            if identifier not in kwargs:
                raise Exception(f"Missing {identifier}")
            self.values[identifier] = kwargs.pop(identifier)
        if self.operation_type in ["create", "update"]:
            shape = F.dict(self.schema)
            if operation_type == "create":
                self.values |= shape.write(kwargs)
            else:
                self.values |= shape.update(kwargs)
        if self.operation_type == "rename":
            self.values.new_name = kwargs.pop("new_name")
            assert not kwargs

    def sql_statements(self):
        if self.operation_type == "create":
            return self.create_sql()
        elif self.operation_type == "update":
            return self.update_sql()
        elif self.operation_type == "delete":
            return self.delete_sql()
        elif self.operation_type == "rename":
            return self.rename_sql(self.values.new_name)
        else:
            assert False


class TableOperation(BaseOperation):
    identifiers = ["name"]
    schema = dict()

    def create_sql(self):
        return [f"CREATE TABLE app.{self.values.name} (id uuid PRIMARY KEY)"]

    def delete_sql(self):
        return [f"DROP TABLE app.{self.values.name}"]


class ColumnOperation(BaseOperation):
    identifiers = ["table_name", "name"]
    schema = dict(
        type=F.text(), nullable=F.bool(), default=F.text(null=True), has_default=F.bool()
    )

    def create_sql(self):
        default_clause = (
            f" DEFAULT {self.values.default}" if self.values.has_default else ""
        )
        return [
            f"ALTER TABLE app.{self.values.table_name} ADD COLUMN {self.values.name} {self.values.type}{'' if self.values.nullable else ' NOT NULL'}{default_clause}"
        ]

    def delete_sql(self):
        return [
            f"ALTER TABLE app.{self.values.table_name} DROP COLUMN {self.values.name}"
        ]

    def update_sql(self):
        assert self.values
        actions = []
        if "nullable" in self.values:
            actions.append(("DROP" if self.values.nullable else "SET") + " NOT NULL")
        if "type" in self.values:
            actions.append("TYPE " + self.values.type)
        if "has_default" in self.values or "default" in self.values:
            if "has_default" in self.values and self.values.has_default is False:
                actions.append("DROP DEFAULT")
            else:
                actions.append(
                    f"SET DEFAULT {self.values.default if 'default' in self.values else 'NULL'}"
                )

        return [
            f"ALTER TABLE app.{self.values.table_name} ALTER COLUMN {self.values.name} {', '.join(actions)}"
        ]


class IndexOperation(BaseOperation):
    identifiers = ["table_name", "name"]
    schema = dict(column_names=F.list(F.text()), is_unique=F.bool())

    def create_sql(self):
        column_names = ", ".join(self.values.column_names)
        return [
            f"CREATE{' UNIQUE' if self.values.is_unique else ''} INDEX {self.values.name} ON app.{self.values.table_name} ({column_names})"
        ]

    def rename_sql(self, new_name):
        return [f"ALTER INDEX app.{self.values.name} RENAME TO {new_name}"]

    def update_sql(self):
        assert False

    def delete_sql(self):
        return [f"DROP INDEX app.{self.values.name}"]


class ForeignKeyConstraintOperation(BaseOperation):
    identifiers = ["table_name", "name"]
    schema = dict(
        column_name=F.text(), referenced_table_name=F.text(), on_delete=F.text()
    )

    def create_sql(self):
        return [
            f"ALTER TABLE app.{self.values.table_name} ADD CONSTRAINT {self.values.name} FOREIGN KEY ({self.values.column_name}) REFERENCES app.{self.values.referenced_table_name}(id) ON DELETE {self.values.on_delete}"
        ]

    def delete_sql(self):
        return [
            f"ALTER TABLE app.{self.values.table_name} DROP CONSTRAINT {self.values.name}"
        ]


def get_needed_operations():
    models = settings.MODEL_GRAPH.all_models()
    code_state = get_code_state(models)
    db_state = get_db_state()
    return get_operations(code_state, db_state)
