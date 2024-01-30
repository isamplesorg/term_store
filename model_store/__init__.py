import dataclasses
import operator
import typing
import uuid
import sqlalchemy
import sqlalchemy.dialects.postgresql
import sqlalchemy.orm
import sqlalchemy.types
import sqlalchemy.sql.expression
import linkml_runtime.utils.yamlutils

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


class Entity(Base):
    """
    Represents a single entity that will usually have an associated globally
    unique identifier.

    """
    __tablename__ = "entity"
    id: sqlalchemy.orm.Mapped[uuid.UUID] = sqlalchemy.orm.mapped_column(
        doc="Unique key for this record. This is an internal identifier.",
        primary_key=True,
        default=uuid.uuid4,
    )
    # An Entity may have more than one globally unique identifier
    pids: sqlalchemy.orm.Mapped[typing.List["GUID"]] = sqlalchemy.orm.relationship(
        back_populates="entity",
        cascade="all, delete-orphan",
        doc="List of GUIDs for this entity."
    )

    # The type of thing, class name
    ttype: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        doc="Class name for this entity",
        index=True,
    )
    # Properties of the Thing
    properties: sqlalchemy.orm.Mapped[dict[str, typing.Any]] = sqlalchemy.orm.mapped_column(
        type_=JSONVariant,
        nullable=True,
        doc="Properties of this entity.",
    )


class GUID(Base):
    """
    A globally unique identifier (i.e. ARK, IGSN, DOI, etc) that references an Entity.
    """
    __tablename__ = "guid"
    id: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        doc="Globally unique PID, DOI, ARK, ROR, ORCID, ...",
        primary_key=True,
    )
    entity_id: sqlalchemy.orm.Mapped[uuid.UUID] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("entity.id")
    )
    entity: sqlalchemy.orm.Mapped["Entity"] = sqlalchemy.orm.relationship(
        back_populates="pids"
    )
    properties: sqlalchemy.orm.Mapped[dict[str, typing.Any]] = sqlalchemy.orm.mapped_column(
        type_=JSONVariant,
        nullable=True,
        doc="Properties of this guid.",
    )


class Relation(Base):
    """
    Defines a relationship between two guids.

    Relationships between entities are expressed as Relation records between the
    GUIDs associated with an Entity, rather than directly with the Entity.
    """
    __tablename__ = "relation"

    src: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("guid.id"),
        primary_key=True,
        doc="Subject of the statement"
    )
    dst: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        sqlalchemy.ForeignKey("guid.id"),
        primary_key=True,
        doc="Object of the statement"
    )
    predicate: sqlalchemy.orm.Mapped[str] = sqlalchemy.orm.mapped_column(
        primary_key=True,
        doc="Predicate of the relationship statement"
    )
    properties: sqlalchemy.orm.Mapped[dict[str, typing.Any]] = sqlalchemy.orm.mapped_column(
        type_=JSONVariant,
        nullable=True,
        doc="Properties of this relationship.",
    )


def get_session(engine: sqlalchemy.Engine):
    return sqlalchemy.orm.sessionmaker(bind=engine)()


def create_database(engine: sqlalchemy.Engine):
    """
    Executes the DDL to set up the database.

    Args:
        engine: Database instance
    """
    Base.metadata.create_all(engine)


def clear_database(engine: sqlalchemy.Engine):
    """
    Drop all tables from the database.

    Args:
        engine: Database instance
    """
    Base.metadata.drop_all(engine)


class ThingRepository:
    """
    Implements the repository pattern for the collection of things that
    are persisted to the database.

    Interactions with the database should occur through this class.
    """

    def __init__(self, session: sqlalchemy.orm.Session):
        self._session = session
        self._thing_identifier_names = ["identifier", "sample_identifier"]

    def add_entity(self, item:Entity) -> uuid.UUID:
        self._session.add(item)
        self._session.commit()
        return item.id

    def get_entity(self, id:uuid.UUID) -> typing.Optional[Entity]:
        record = self._session.get(GUID, id)
        return record

    def add_guid(self, item:GUID):
        self._session.add(item)
        self._session.commit()
        return item.id

    def add_relation(self, item:Relation):
        self._session.add(item)
        self._session.commit()
        return (item.src, item.predicate, item.dst)

    def add_term(self, item:Term):
        self._session.add(item)
        self._session.commit()
        return item.uri

    def get_term(self, uri:str) -> typing.Optional[Term]:
        record = self._session.get(Term, uri)
        return record

    def broader_terms(self, start_uri: str) -> typing.Generator[Term, None, None]:
        """Get terms that are broader than the provided URI
        """
        #sql = sqlalchemy.text("""WITH RECURSIVE cte(uri, scheme, name, broader, properties) AS (
        #SELECT term.uri AS uri, term.scheme AS scheme, term.name AS name, term.broader AS broader, term.properties AS properties
        #FROM term
        #WHERE term.uri = :start_uri
        #UNION
        #SELECT term.uri AS term_uri, term.scheme AS term_scheme, term.name AS term_name, term.broader AS term_broader, term.properties AS term_properties
        #FROM term
        #JOIN cte ON (
        #    cte.broader LIKE '%' || term.uri || '%'
        #)) SELECT cte.uri AS cte_uri, cte.scheme AS cte_scheme, cte.name AS cte_name, cte.broader AS cte_broader, cte.properties AS cte_properties
        #FROM cte""")
        q = self._session.query(Term)
        q = q.filter(Term.uri == start_uri)
        q = q.cte('cte', recursive=True)
        q2 = self._session.query(Term)
        q2 = q2.join(q, q.c.broader.contains(Term.uri))
        rq = q.union(q2)
        qq = self._session.query(rq)
        #qq = self._session.execute(sql, {"start_uri":start_uri})
        for record in qq:
            yield record

    def narrower_terms(self, start_uri) -> typing.Generator[Term, None, None]:
        q = self._session.query(Term)
        q = q.filter(Term.broader.contains(start_uri))
        q = q.cte('cte', recursive=True)
        q2 = self._session.query(Term)
        q2 = q2.join(q, Term.broader.contains(q.c.uri))
        rq = q.union(q2)
        qq = self._session.query(rq)
        for record in qq:
            yield record

    def term_schemes(self) -> typing.Generator[typing.Tuple[str, int], None, None]:
        sql = sqlalchemy.text("SELECT scheme, count(*) AS cnt FROM term GROUP BY scheme;")
        #q = self._session.query(sqlalchemy.distinct(Term.scheme), sqlalchemy.func.count(Term.scheme))
        q = self._session.execute(sql)
        for record in q:
            yield (record)

    def _thing_components(self, thing:linkml_runtime.utils.yamlutils.YAMLRoot, parent_id:str=None):
        components = []
        cn = thing.__class__.__name__
        uid = uuid.uuid4().hex
        entity = {
            "id": uid,
            "ttype": thing.__class__.__name__,
            "properties": {},
            "pid": parent_id,
            "cid": [],
            "identifiers": []
        }
        components.append(entity)
        field_names = [f.name for f in dataclasses.fields(thing)]
        for fn in field_names:
            v = getattr(thing, fn)
            if fn in self._thing_identifier_names:
                entity["identifiers"].append(v)
            if isinstance(v, list):
                entity["properties"][fn] = []
                for vi in v:
                    if isinstance(vi, linkml_runtime.utils.yamlutils.YAMLRoot):
                        components += self._thing_components(vi, entity["_id"])
                    else:
                        entity["props"][fn].append(vi)
            else:
                if isinstance(v, linkml_runtime.utils.yamlutils.YAMLRoot):
                    components += self._thing_components(v, entity["_id"])
                else:
                    entity["props"][fn] = v
        for e in components:
            if e["pid"] == entity["_id"]:
                entity["cid"].append(e["_id"])
        return components

    def create(self, thing:linkml_runtime.utils.yamlutils.YAMLRoot) -> typing.List[str]:
        """
        Adds the thing to the data store.

        The thing is based on YAMLRoot as that is the base class of the LinkML
        defined things. A "thing" may be an instance of any isamples_core
        model defined class. e.g. PhysicalSampleRecord, Agent, SamplingSite

        The process is a bit complicated and model dependent, because not all
        parts get split out into entities. For example, Keyword and Identifier
        instances should not be treated as entities, but Agent, SamplingSite,
        SamplingEvent, GeospatialCoordLocation, and PhysicalSampleRecord are.

        If a complex thing, then the components are stored as separate entities.

        The uuids of all entities added is returned.
        """
        # get identifier of thing, check that not already
        components = self._thing_components(thing)
        for component in components:
            pass

    def read(self, guid:str)-> linkml_runtime.utils.yamlutils.YAMLRoot:
        pass

    def update(self, thing:linkml_runtime.utils.yamlutils.YAMLRoot):
        pass

    def delete(self, guid:str):
        pass