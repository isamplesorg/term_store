import typing
import sqlalchemy.orm

from .db import Term

class TermRepository:
    """
    Implements the repository pattern for the collection of vocabulary terms
    persisted in the database.
    """

    def __init__(self, session: sqlalchemy.orm.Session):
        self._session = session

    def add(self, item:Term):
        self._session.add(item)
        self._session.commit()
        return item.uri

    def read(self, uri:str) -> typing.Optional[Term]:
        """
        Retrieve the Term with the given URI.

        Args:
            uri: URI identifying the term to retrieve.

        Returns:
            Term or None if URI doesn't match anything.
        """
        record = self._session.get(Term, uri)
        return record

    def broader(self, start_uri: str) -> typing.Generator[Term, None, None]:
        """Get terms that are broader than the provided URI.
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

    def narrower(self, start_uri) -> typing.Generator[Term, None, None]:
        q = self._session.query(Term)
        q = q.filter(Term.broader.contains(start_uri))
        q = q.cte('cte', recursive=True)
        q2 = self._session.query(Term)
        q2 = q2.join(q, Term.broader.contains(q.c.uri))
        rq = q.union(q2)
        qq = self._session.query(rq)
        for record in qq:
            yield record

    def schemes(self) -> typing.Generator[typing.Tuple[str, int], None, None]:
        sql = sqlalchemy.text("SELECT scheme, count(*) AS cnt FROM term GROUP BY scheme;")
        #q = self._session.query(sqlalchemy.distinct(Term.scheme), sqlalchemy.func.count(Term.scheme))
        q = self._session.execute(sql)
        for record in q:
            yield (record)
