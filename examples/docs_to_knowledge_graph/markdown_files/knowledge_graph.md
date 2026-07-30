# Knowledge Graphs

A knowledge graph represents information as entities and the relationships
between them. Synor builds a knowledge graph by extracting relationships
from documents and writing them to a graph database.

LLM extraction turns unstructured text into structured data. Synor uses an
LLM to read a document and produce a summary together with a set of
`(subject, predicate, object)` triples about the concepts it covers.

A triple becomes a relationship in the graph. The subject and object become
entities, and the predicate labels the edge between them. Synor also records
which document mentioned which entity.

Neo4j stores the resulting property graph. Synor declares Document nodes,
Entity nodes, and the edges between them, and incremental processing keeps the
knowledge graph up to date as the source documents change.
