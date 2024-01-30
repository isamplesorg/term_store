"""
Tests for loading skos vocab into the term_store
"""

import typing
import pytest
import sqlalchemy
import term_store
import term_store.vocab_terms
import term_store.repository

@pytest.fixture(scope="module")
def sqlite_repo() -> typing.Iterator[term_store.repository.TermRepository]:
    cnstr = "sqlite:///"
    engine = sqlalchemy.create_engine(cnstr, echo=False)
    term_store.create_database(engine)
    session = term_store.get_session(engine)
    repo = term_store.get_repository(session)
    yield repo
    session.close()


def test_load_skos(sqlite_repo):
    src = "/Users/vieglais/Documents/Projects/isamples/source/model/vocabularies/src/materialtype.ttl"
    vocab = term_store.vocab_terms.SKOSVocabulary()
    vocab.load(src)
    vocab.load_terms_to_model_store(sqlite_repo)
    for t in sqlite_repo.broader("https://w3id.org/isample/vocabulary/material/0.9/anthropogenicmetal"):
        print(t.name)
