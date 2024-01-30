import pytest
import sqlalchemy
import model_store


@pytest.fixture
def sqlite_repo():
    cnstr = "sqlite:///:memory:"
    engine = sqlalchemy.create_engine(cnstr, echo=True)
    model_store.create_database(engine)
    session = model_store.get_session(engine)
    repo = model_store.ThingRepository(session)
    yield repo
    session.close()


@pytest.mark.usefixtures("sqlite_repo")
def test_add_entity(sqlite_repo):
    pids = [
        model_store.GUID(
            id="ark:99999/test/01",
            properties={}
        )
    ]
    entity = model_store.Entity(
        pids=pids,
        ttype = "Test",
        properties = {"a":"b"}
    )
    #sqlite_repo.add_all([entity,])
    #sqlite_repo.commit()


@pytest.mark.usefixtures("sqlite_repo")
def test_add_term(sqlite_repo):
    sqlite_repo.add_term(model_store.Term(
        uri="urn:foo:root",
        scheme="urn:foo",
        name="root",
    ))
    sqlite_repo.add_term(model_store.Term(
        uri="urn:foo:c1",
        scheme="urn:foo",
        name="c1",
        broader=["urn:foo:root",]
    ))
    sqlite_repo.add_term(model_store.Term(
        uri="urn:foo:c11",
        scheme="urn:foo",
        name="c11",
        broader=["urn:foo:c1"],
    ))
    sqlite_repo.add_term(model_store.Term(
        uri="urn:foo:c111",
        scheme="urn:foo",
        name="c111",
        broader = ["urn:foo:c11"],
    ))
    sqlite_repo.add_term(model_store.Term(
        uri="urn:foo:c12",
        scheme="urn:foo",
        name="c12",
        broader=["urn:foo:c1"],
    ))
    for t in sqlite_repo.broader_terms("urn:foo:c11"):
        print(t)
    print("============")
    for t in sqlite_repo.narrower_terms("urn:foo:root"):
        print(t)
    print("=========")
    for t in sqlite_repo.term_schemes():
        print(t)
