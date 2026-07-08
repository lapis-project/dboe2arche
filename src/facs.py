import glob
import json
from collections import defaultdict

from acdh_tei_pyutils.tei import TeiReader
from acdh_tei_pyutils.utils import get_xmlid

files = glob.glob("data/*.xml")

data = defaultdict(list)

for x in files:
    doc = TeiReader(x)
    print(f"processing {x}")
    for entry in doc.any_xpath(".//tei:entry[./@facs]"):
        facs = entry.attrib["facs"].split()
        data[get_xmlid(entry)] = facs

with open("facs.json", "w", encoding="utf-8") as fp:
    json.dump(data, fp, ensure_ascii=False)
