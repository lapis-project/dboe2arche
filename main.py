import glob
import os
import shutil

from rdflib import RDF, Graph, Literal, Namespace, URIRef
from tqdm import tqdm
from acdh_tei_pyutils.tei import TeiReader
from acdh_tei_pyutils.utils import any_xpath

to_ingest = "to_ingest"
out_file = os.path.join(to_ingest, "arche.ttl")
shutil.rmtree(to_ingest, ignore_errors=True)
os.makedirs(to_ingest, exist_ok=True)
g = Graph().parse("arche/arche_top_col.ttl")
arche_constants = Graph().parse("arche/arche_constants.ttl")
TOP_COL = os.environ.get("TOPCOLID", "https://id.acdh.oeaw.ac.at/dboe-tei-xml")
TOP_COL_URI = URIRef(TOP_COL)
ACDH = Namespace("https://vocabs.acdh.oeaw.ac.at/schema#")
DBOE_XMLS = "/home/csae8092/repos/dboe/dboe_orig_xml/*.xml"

files_to_ingest = sorted(glob.glob(DBOE_XMLS))

for x in tqdm(files_to_ingest[30:40]):    
    f_name = os.path.basename(x)
    subj = URIRef(f"{TOP_COL_URI}/{f_name}")
    g.add((subj, RDF.type, ACDH["Resource"]))
    for p, o in arche_constants.predicate_objects():
        g.add((subj, p, o))
    g.add(
        (
            subj,
            ACDH["hasCategory"],
            URIRef("https://vocabs.acdh.oeaw.ac.at/archecategory/text/tei"),
        )
    )
    g.add(
        (
            subj,
            ACDH["isPartOf"],
            TOP_COL_URI,
        )
    )
    doc = TeiReader(x)
    items = doc.any_xpath(".//tei:entry[@xml:id]")
    first_lemma = any_xpath(items[0], "./tei:form[1]/tei:orth/text()")[0]
    last_lemma = any_xpath(items[-1], "./tei:form[1]/tei:orth/text()")[0]
    title = f"{first_lemma} – {last_lemma} (Datei: {f_name})"
    g.add((subj, ACDH["hasTitle"], Literal(title, lang="de")))
    g.add((subj, ACDH["hasExtent"], Literal(f"{len(items)} Einträge")))

g.serialize(out_file)