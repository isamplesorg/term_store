"""
Tests for term CRUD and hierarchy
"""

import pytest
import sqlalchemy
import term_store

@pytest.fixture(scope="module")
def sqlite_repo():
    cnstr = "sqlite:///:memory:"
    engine = sqlalchemy.create_engine(cnstr, echo=False)
    term_store.create_database(engine)
    session = term_store.get_session(engine)
    repo = term_store.get_repository(session)
    yield repo
    session.close()


@pytest.fixture(scope="module")
def load_terms(sqlite_repo):
    """
    root <- c1 <- c11 <- c111
            ^
            +- c12
            +- f1
    """
    sqlite_repo.add(term_store.db.Term(
        uri="urn:foo:root",
        scheme="urn:foo",
        name="root",
    ))
    sqlite_repo.add(term_store.db.Term(
        uri="urn:foo:c1",
        scheme="urn:foo",
        name="c1",
        broader=["urn:foo:root",]
    ))
    sqlite_repo.add(term_store.db.Term(
        uri="urn:foo:c11",
        scheme="urn:foo",
        name="c11",
        broader=["urn:foo:c1"],
    ))
    sqlite_repo.add(term_store.db.Term(
        uri="urn:foo:c111",
        scheme="urn:foo",
        name="c111",
        broader = ["urn:foo:c11"],
    ))
    sqlite_repo.add(term_store.db.Term(
        uri="urn:foo:c12",
        scheme="urn:foo",
        name="c12",
        broader=["urn:foo:c1"],
    ))
    sqlite_repo.add(term_store.db.Term(
        uri="urn:bar:f1",
        scheme="urn:bar",
        name="f1",
        broader=["urn:foo:c1"],
    ))


def test_broader_terms(sqlite_repo, load_terms):
    results = [t.name for t in sqlite_repo.broader("urn:foo:c11")]
    expected = ["c11", "c1", "root"]
    for i in range(0, len(expected)):
        assert results[i] == expected[i]


def test_narrower_terms(sqlite_repo, load_terms):
    results = [t.name for t in sqlite_repo.narrower("urn:foo:root")]
    expected = ["c1", "c11", "c111", "c12", "f1"]
    for i in range(0, len(expected)):
        assert results[i] == expected[i]


def test_term_schemes(sqlite_repo, load_terms):
    results = [t for t in sqlite_repo.schemes()]
    expected = [("urn:foo", 6), ("urn:bar",1), ]
    assert len(results) == len(expected)
    for e in expected:
        assert e[0] in [s[0] for s in results]
