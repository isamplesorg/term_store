import typing
import sqlalchemy.dialects.postgresql
import sqlalchemy.orm
import sqlalchemy.sql.expression


# uses JSONB when DB is postgres, otherwise JSON
JSONVariant = sqlalchemy.types.JSON().with_variant(
    sqlalchemy.dialects.postgresql.JSONB(), "postgresql"
)


class Base(sqlalchemy.orm.DeclarativeBase):
    pass


class Term(Base):
    """
    Represents the collection of vocabulary terms and keywords used by
    all entities. Terms support simple broader / narrower relationship
    (ala SKOS). Arbitrary additional properties may be associated with
    each term through the properties dictionary.
    """

    __tablename__ = "term"
    uri: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        doc="Term URI. Unique across all terms.", primary_key=True
    )
    scheme: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        doc="Scheme that this term appears in", index=True
    )
    name: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        doc="Term value, skos:prefValue", index=True
    )
    broader: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        nullable=True,
        doc="broader terms of this term, comma-separated.", index=True
    )
    properties: sqlalchemy.orm.Mapped[
        dict[str, typing.Any]
    ] = sqlalchemy.orm.mapped_column(
        type_=JSONVariant,
        nullable=True,
        doc="Properties of this term.",
    )
