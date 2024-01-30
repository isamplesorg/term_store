import operator
import typing
import uuid
import sqlalchemy
import sqlalchemy.dialects.postgresql
import sqlalchemy.orm
import sqlalchemy.types
import sqlalchemy.sql.expression

# https://gist.github.com/gmolveau/7caeeefe637679005a7bb9ae1b5e421e
class UUID(sqlalchemy.types.TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type or MSSQL's UNIQUEIDENTIFIER,
    otherwise uses CHAR(32), storing as stringified hex values.

    https://docs.sqlalchemy.org/en/20/core/custom_types.html#backend-agnostic-guid-type
    """

    impl = sqlalchemy.types.CHAR
    cache_ok = True

    _default_type = sqlalchemy.types.CHAR(32)
    _uuid_as_str = operator.attrgetter("hex")

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(sqlalchemy.dialects.postgresql.UUID())
        elif dialect.name == "mssql":
            return dialect.type_descriptor(sqlalchemy.dialects.mssql.UNIQUEIDENTIFIER())
        else:
            return dialect.type_descriptor(self._default_type)

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name in ("postgresql", "mssql"):
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return self._uuid_as_str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value


# uses JSONB when DB is postgres, otherwise JSON
JSONVariant = sqlalchemy.types.JSON().with_variant(sqlalchemy.dialects.postgresql.JSONB(), 'postgresql')

class Base(sqlalchemy.orm.DeclarativeBase):
    type_annotation_map = {
        uuid.UUID: UUID,
    }


class Term(Base):
    """
    Represents the collection of vocabulary terms and keywords used by
    all entities. Terms support simple broader / narrower relationship
    (ala SKOS). Arbitrary additional properties may be associated with
    each term through the properties dictionary.
    """
    __tablename__ = "term"
    uri: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        doc="Term URI. Unique across all terms.",
        primary_key=True
    )
    scheme: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        doc="Scheme that this term appears in",
        index = True
    )
    name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        doc="Term value, skos:prefValue",
        index=True
    )
    broader: sqlalchemy.orm.Mapped[list[str]] = sqlalchemy.orm.mapped_column(
        type_= JSONVariant,
        nullable=True,
        doc="broader terms of this term.",
    )
    properties: sqlalchemy.orm.Mapped[dict[str, typing.Any]] = sqlalchemy.orm.mapped_column(
        type_= JSONVariant,
        nullable=True,
        doc="Properties of this term.",
    )
