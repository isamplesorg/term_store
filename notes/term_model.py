'''
Implements an SQL model for controlled vocabularies.
'''

import typing
import sqlalchemy
import sqlalchemy.orm
import model_store

class Term(model_store.Base):
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
        type_= model_store.JSONVariant,
        nullable=True,
        doc="broader terms of this term.",
    )

    properties: sqlalchemy.orm.Mapped[dict[str, typing.Any]] = sqlalchemy.orm.mapped_column(
        type_= model_store.JSONVariant,
        nullable=True,
        doc="Properties of this term.",
    )

def broader_terms(session, than_uri):
    #term = session.get(Term, than_uri)
    #scheme = term.scheme
    q = session.query(Term)
    q = q.filter(Term.uri==than_uri)
    q = q.cte('cte', recursive=True)

    q2 = session.query(Term)
    q2 = q2.join(q, q.c.broader.contains(Term.uri))

    rq = q.union(q2)
    qq = session.query(rq)
    for record in qq.all():
        print(record)

def narrower_terms(session, than_uri):
    q = session.query(Term)
    q = q.filter(Term.broader.contains(than_uri))
    q = q.cte('cte', recursive=True)

    q2 = session.query(Term)
    q2 = q2.join(q, Term.broader.contains(q.c.uri))

    rq = q.union(q2)
    qq = session.query(rq)
    for record in qq.all():
        print(record)
