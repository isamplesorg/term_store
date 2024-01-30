"""
term_store provides a mechanism for storing SKOS vocabulary terms as simplified
structures in a sqlalchemy database instance.

Basic use is like:

session = get_session(engine)
repository = get_repository(session)

"""
import sqlalchemy
import sqlalchemy.orm

from .db import Base
from .repository import TermRepository

__version__ = "0.2.0"


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


def get_session(engine: sqlalchemy.Engine) -> sqlalchemy.orm.Session:
    return sqlalchemy.orm.sessionmaker(bind=engine)()


def get_repository(session: sqlalchemy.orm.Session) -> TermRepository:
    """
    Retrieve a repository instance given a database session.
    """
    return TermRepository(session)
