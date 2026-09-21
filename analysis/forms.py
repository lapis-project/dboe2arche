import glob
import json

from acdh_tei_pyutils.tei import TeiReader
from acdh_tei_pyutils.utils import any_xpath, get_xmlid

try:
    from analysis.utils import node_to_json
except ModuleNotFoundError:
    from utils import node_to_json

items = {}
files = sorted(glob.glob("./data/f*.xml"))
attribute_names = set()
for x in files:
    doc = TeiReader(x)
    print(f"processing {x}")
    for y in doc.any_xpath(".//tei:entry[./tei:form[@type='verweislemma']]"):
        try:
            xml_id = get_xmlid(y)
        except KeyError:
            continue
        items[xml_id] = []
        for node in any_xpath(y, "./tei:form[@type='verweislemma']"):
            items[xml_id].append(node_to_json(node))

all_keys = set()

# go through def items and save their first-level key names
for x in items.values():
    for y in x:
        all_keys.update(y)

with open("forms_verweislemma.json", "w", encoding="utf-8") as fp:
    json.dump(items, fp, ensure_ascii=False, indent=4)

with open("forms_verweislemma_attribute_names.json", "w", encoding="utf-8") as fp:
    json.dump(sorted(all_keys), fp, ensure_ascii=False, indent=4)

print(len(items))
