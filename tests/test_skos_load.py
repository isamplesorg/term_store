"""
Tests for loading skos vocab into the term_store
"""

import typing
import pytest
import sqlalchemy
import term_store
import term_store.vocab_terms
import term_store.repository

@pytest.fixture(scope="function")
def sqlite_repo() -> typing.Iterator[term_store.repository.TermRepository]:
    cnstr = "sqlite:///"
    engine = sqlalchemy.create_engine(cnstr, echo=False)
    term_store.create_database(engine)
    session = term_store.get_session(engine)
    repo = term_store.get_repository(session)
    yield repo
    session.close()


def test_load_skos(sqlite_repo):
    src = "https://raw.githubusercontent.com/isamplesorg/vocab_tools/main/docsrc/example/data/example.ttl"
    vocab = term_store.vocab_terms.SKOSVocabulary()
    vocab.load(src)
    vocab.load_terms_to_model_store(sqlite_repo)
    expected = [
        "https://example.net/my/minimal/thing",
        "https://example.net/my/minimal/solid",
    ]
    actual = []
    for t in sqlite_repo.broader("https://example.net/my/minimal/solid"):
        actual.append(t.uri)
    assert len(actual) == len(expected)
    for uri in actual:
        assert uri in expected


def test_load_skos_extension(sqlite_repo):
    vocab = term_store.vocab_terms.SKOSVocabulary()
    vocab.load("https://raw.githubusercontent.com/isamplesorg/vocab_tools/main/docsrc/example/data/example.ttl")
    vocab.load("https://raw.githubusercontent.com/isamplesorg/vocab_tools/main/docsrc/example/data/extension_example.ttl")
    vocab.load_terms_to_model_store(sqlite_repo)
    expected = [
        "https://example.net/my/minimal/thing",
        "https://example.net/my/extension/liquid",
    ]
    actual = []
    for t in sqlite_repo.broader("https://example.net/my/extension/liquid"):
        actual.append(t.uri)
    assert len(actual) == len(expected)
    for uri in actual:
        assert uri in expected
    expected = [
        "https://example.net/my/extension/water",
    ]
    actual = []
    for t in sqlite_repo.narrower("https://example.net/my/extension/liquid"):
        actual.append(t.uri)
    assert len(actual) == len(expected)
    for uri in actual:
        assert uri in expected

