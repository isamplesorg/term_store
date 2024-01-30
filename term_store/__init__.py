import sqlalchemy
import sqlalchemy.orm

from .db import Base
from .repository import TermRepository

def get_session(engine: sqlalchemy.Engine) -> sqlalchemy.orm.Session:
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


def get_repository(session: sqlalchemy.orm.Session) -> TermRepository:
    return TermRepository(session)
