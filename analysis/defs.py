import glob
import json

from acdh_tei_pyutils.tei import TeiReader
from acdh_tei_pyutils.utils import any_xpath, get_xmlid

more_than_one_def = {}
files = sorted(glob.glob("./data/*.xml"))
for x in files:
    doc = TeiReader(x)
    print(f"processing {x}")
    for y in doc.any_xpath(".//tei:cit[./tei:def[2]]"):
        try:
            xml_id = get_xmlid(y)
        except KeyError:
            continue
        more_than_one_def[xml_id] = []
        for tei_def in any_xpath(y, ".//tei:def"):
            item = {"text": tei_def.text}
            item.update(tei_def.attrib)
            more_than_one_def[xml_id].append(item)

with open("defs.json", "w", encoding="utf-8") as fp:
    json.dump(more_than_one_def, fp, ensure_ascii=False, indent=4)

print(len(more_than_one_def))
