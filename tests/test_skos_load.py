"""
Tests for loading skos vocab into the model_store
"""

import pytest
import sqlalchemy
import model_store
import model_store.vocab_terms


@pytest.fixture(scope="module")
def sqlite_repo():
    cnstr = "sqlite:///test.sqlite"
    engine = sqlalchemy.create_engine(cnstr, echo=False)
    model_store.create_database(engine)
    session = model_store.get_session(engine)
    repo = model_store.ThingRepository(session)
    yield repo
    session.close()


def test_load_skos(sqlite_repo):
    src = "/Users/vieglais/Documents/Projects/isamples/source/model/vocabularies/src/materialtype.ttl"
    vocab = model_store.vocab_terms.SKOSVocabulary()
    vocab.load(src)
    vocab.load_terms_to_model_store(sqlite_repo)
    for t in sqlite_repo.broader_terms("https://w3id.org/isample/vocabulary/material/0.9/anthropogenicmetal"):
        print(t.name)
