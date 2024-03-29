"""
Utility methods for loading vocabularies to the term_store.
"""

import dataclasses
import logging
import typing
import rdflib
import rdflib.namespace
import rdflib.plugins.sparql
from .db import Term
from .repository import TermRepository

#TODO: this is too specific:
STORE_IDENTIFIER = "https://w3id.org/isample/vocabulary"

#TODO: should use namespaces from rdflib
NS = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl": "http://www.w3.org/2002/07/owl#",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "obo": "http://purl.obolibrary.org/obo/",
    "geosciml": "http://resource.geosciml.org/classifier/cgi/lithology",
}

L = logging.getLogger("vocab_terms")

def skosT(term):
    return rdflib.URIRef(f"{NS['skos']}{term}")


def rdfT(term):
    return rdflib.URIRef(f"{NS['rdf']}{term}")


def rdfsT(term):
    return rdflib.URIRef(f"{NS['rdfs']}{term}")


@dataclasses.dataclass
class VocabularyConcept:
    uri: str
    name: str  # Last part of the URI, foo#name or foo/name
    label: typing.List[str]
    definition: str
    broader: typing.List[str]
    narrower: typing.List[str]
    vocabulary: str
    history: typing.List[str] = dataclasses.field(default_factory=list)
    notes: typing.List[str] = dataclasses.field(default_factory=list)
    scopenote: typing.List[str] = dataclasses.field(default_factory=list)
    related: typing.List[str] = dataclasses.field(default_factory=list)
    example: typing.List[str] = dataclasses.field(default_factory=list)
    changenote: typing.List[str] = dataclasses.field(default_factory=list)

class SKOSVocabulary:
    # helper for making SPARQL queries
    _PFX = f"""
    PREFIX skos: <{NS['skos']}>
    PREFIX owl: <{NS['owl']}>
    PREFIX rdf: <{NS['rdf']}>
    PREFIX rdfs: <{NS['rdfs']}>
    """
    DEFAULT_FORMAT = "text/turtle"
    DEFAULT_STORE = "default"

    def __init__(
            self,
            storage_uri=DEFAULT_STORE,
            store_identifier=STORE_IDENTIFIER,
            purge_existing=False,
    ):
        self.origin = None
        self.storage_uri = storage_uri
        self.store_identifier = store_identifier
        self._g = None
        self._terms = {}
        self._literals = ""
        self._initialize_store(purge=purge_existing)

    def __len__(self):
        return len(self._g)

    def _initialize_store(self, purge=False, store="default"):
        graph = rdflib.ConjunctiveGraph(store=store, identifier=self.store_identifier)
        if purge:
            graph.destroy(self.storage_uri)
        graph.open(self.storage_uri, create=True)
        self._g = graph

    @property
    def graph(self):
        return self._g

    def query(self, q, **bindings):
        L.debug(q)
        sparql = rdflib.plugins.sparql.prepareQuery(SKOSVocabulary._PFX + q)
        return self._g.query(sparql, initBindings=bindings)

    def expand_name(self, n: typing.Optional[str]) -> typing.Optional[str]:
        if n is None:
            return n
        try:
            return self._g.namespace_manager.expand_curie(n)
        except (ValueError, TypeError):
            pass
        return n

    def compact_name(self, n: typing.Optional[str]) -> typing.Optional[str]:
        if n is None:
            return n
        try:
            return rdflib.URIRef(n).n3(self._g.namespace_manager)
        except (ValueError, TypeError):
            pass
        return n

    def _one_res(self, rows, abbreviate=False) -> list[str]:
        res = []
        for r in rows:
            if abbreviate:
                res.append(self.compact_name(r[0]))
            else:
                res.append(r[0])
        return res

    def _result_single_value(self, rows, abbreviate=False) -> typing.Any:
        for r in rows:
            if abbreviate:
                return self.compact_name(r[0])
            return r[0]
        return None

    def _get_objects(self, subject: str, predicate: str) -> list[str]:
        q = """SELECT ?o
        WHERE {
            ?subject ?predicate ?o .
        }"""
        qres = self.query(q, subject=subject, predicate=predicate)
        res = []
        for row in qres:
            res.append(row[0])
        return res

    def objects(self, subject: str, predicate: str) -> list[str]:
        res = []
        qres = self._get_objects(subject, predicate)
        for row in qres:
            v = row
            v = str(v).strip()
            if len(v) > 0:
                res.append(v)
        return res

    def broader(
        self, concept: str, v: typing.Optional[str] = None, abbreviate: bool = False
    ) -> list[str]:
        concept = rdflib.URIRef(self.expand_name(concept))
        if v is None:
            q = """SELECT ?s
            WHERE {
                ?child skos:broader ?s .
            }"""
            qres = self.query(q, child=concept)
        else:
            v = self.expand_name(v)
            q = """SELECT ?s
            WHERE {
                ?s skos:inScheme ?vocabulary .
                ?child skos:broader ?s .
            }"""
            qres = self.query(q, vocabulary=v, child=concept)
        res = []
        # Should only ever be a single broader term in a well constructed taxonomy,
        # but who knows how well these things are constructed?
        return self._one_res(qres, abbreviate=abbreviate)

    def narrower(
        self, concept: str, v: typing.Optional[str] = None, abbreviate: bool = False
    ) -> list[str]:
        concept = rdflib.URIRef(self.expand_name(concept))
        if v is None:
            q = """SELECT ?s
            WHERE {
                ?s skos:broader ?parent .
            }"""
            qres = self.query(q, parent=concept)
        else:
            v = self.expand_name(v)
            q = """SELECT ?s
            WHERE {
                ?s skos:inScheme ?vocabulary .
                ?s skos:broader ?parent .
            }"""
            qres = self.query(q, vocabulary=v, parent=concept)
        return self._one_res(qres, abbreviate=abbreviate)

    def load(
        self,
        source: str,
        format: str = DEFAULT_FORMAT,
        bindings: typing.Optional[dict] = None,
    ):
        """
        Loads a vocabulary into the store.

        """
        g_loaded = self._g.parse(source, format=format)
        if bindings is not None:
            for k, v in bindings.items():
                self._g.bind(k, v)
        # Figure the broader concept vocabularies.
        # First check for extension_vocab rdfs:subPropertyOf extended_vocab
        # if not present, then compute and add it for later use.
        # What vocabulary did we just load?
        q = (
            SKOSVocabulary._PFX
            + """SELECT ?s
        WHERE {
            ?s rdf:type skos:ConceptScheme .
        }"""
        )
        qres = g_loaded.query(q)
        loaded_vocabulary = self._result_single_value(qres, abbreviate=False)
        if loaded_vocabulary is not None:
            L.info("Loaded vocabulary %s", loaded_vocabulary)
            q = (
                SKOSVocabulary._PFX
                + """SELECT ?extended
            WHERE {
                ?vocabulary rdfs:subPropertyOf ?extended .
            }"""
            )
            qres = self._g.query(q, initBindings={"vocabulary": loaded_vocabulary})
            _extended_vocab = self._result_single_value(qres, abbreviate=False)
            if _extended_vocab is not None:
                L.info("Extends: %s", _extended_vocab)
                return
            # The extended vocabulary is not specified
            # Figure it by examining the broader concepts for each
            # concept in the loaded vocabulary
            q = (
                SKOSVocabulary._PFX
                + """SELECT ?s
            WHERE {
                ?child rdf:type skos:Concept .
                ?child skos:broader ?s .
            }"""
            )
            qres = g_loaded.query(q)
            broader_concepts = self._one_res(qres)
            vocabs = set()
            for c in broader_concepts:
                concept = self.concept(str(c))
                if concept.vocabulary is not None:
                    vocabs.add(concept.vocabulary)
            for vocab in vocabs:
                if str(vocab) != str(loaded_vocabulary):
                    L.info("Extends: %s", vocab)
                    self._g.add(
                        (
                            rdflib.URIRef(loaded_vocabulary),
                            rdfsT("subPropertyOf"),
                            rdflib.URIRef(vocab),
                        )
                    )
                    self._g.commit()
            return
        L.warning("Loaded vocabulary does not specify skos:ConceptScheme")


    def concept(self, term: str):
        """Given a URI, return the matching VocabularyConcept

        Raises KeyError if not found.
        """
        term = self.expand_name(term)
        if "#" in term:
            ab = term.split("#")
        else:
            ab = term.split("/")
        name = ab[-1]
        labels = self.objects(term, skosT("prefLabel"))
        labels += self.objects(term, skosT("altLabel"))
        labels += self.objects(term, rdfT("label"))
        tmp = self.objects(term, skosT("definition"))
        definition = "\n".join(tmp)
        broader = self.objects(term, skosT("broader"))
        narrower = self.narrower(term)
        notes = self.objects(term, skosT("note"))
        vocabulary = self.objects(term, skosT("inScheme"))
        if len(vocabulary) > 0:
            vocabulary = vocabulary[0]
        else:
            # May be set with topConceptOf
            vocabulary = self.objects(term, skosT("topConceptOf"))
            if len(vocabulary) > 0:
                vocabulary = vocabulary[0]
            else:
                vocabulary = None
        history = self.objects(term, skosT("historyNote"))
        return VocabularyConcept(
            uri=str(term),
            name=name,
            label=labels,
            definition=definition.strip(),
            broader=broader,
            narrower=narrower,
            vocabulary=vocabulary,
            history=history,
            notes=notes,
            scopenote=self.objects(term, skosT("changeNote")),
            related=self.objects(term, skosT("related")),
            example=self.objects(term, skosT("example")),
            changenote=self.objects(term, skosT("changeNote")),
        )


    def concept_uris(
        self, v: typing.Optional[str] = None, abbreviate: bool = False
    ) -> list[str]:
        """List the concept URIs in the specific vocabulary.

        Returns a list of the skos:Concept instances in the specified vocabulary
        as it exists in the current graph store.
        """
        try:
            v = self._g.namespace_manager.expand_curie(v)
        except (ValueError, TypeError):
            pass
        if v is None:
            q = """SELECT ?s
            WHERE {
                ?s rdf:type skos:Concept .
            }"""
            qres = self.query(q)
        else:
            q = """SELECT ?s
                WHERE {
                    ?s skos:inScheme | skos:topConceptOf ?vocabulary .
                    ?s rdf:type skos:Concept .
                }"""
            qres = self.query(q, vocabulary=v)
        return self._one_res(qres, abbreviate=abbreviate)


    def load_terms_to_model_store(self, repository: TermRepository):
        for concept_uri in self.concept_uris():
            concept = self.concept(concept_uri)
            _properties = {
                "labels": concept.label,
                "definition": concept.definition,
            }
            record = Term(
                uri = concept_uri,
                scheme = concept.vocabulary,
                name = concept.name,
                broader = ",".join(concept.broader),
                properties = _properties
            )
            repository.add(record)
