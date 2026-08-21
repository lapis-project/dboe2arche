import glob
import json

from acdh_tei_pyutils.tei import TeiReader
from acdh_tei_pyutils.utils import any_xpath, extract_fulltext_with_spacing, get_xmlid

items = {}
files = sorted(glob.glob("./data/*.xml"))[:2]
attribute_names = set()
for x in files:
    doc = TeiReader(x)
    print(f"processing {x}")
    for y in doc.any_xpath(".//tei:entry[./tei:note]"):
        try:
            xml_id = get_xmlid(y)
        except KeyError:
            continue
        items[xml_id] = []
        for node in any_xpath(y, ".//tei:note"):
            item = {"text": extract_fulltext_with_spacing(node)}
            for attr_name, attr_value in node.attrib.items():
                item[attr_name.split("}")[-1]] = attr_value
                attribute_names.add(attr_name.split("}")[-1])
            for child in node:
                tag_name = child.tag.split("}")[-1]
                attribute_names.add(tag_name)
                item.setdefault(tag_name, []).append(
                    extract_fulltext_with_spacing(child)
                )
            items[xml_id].append(item)

with open("notes.json", "w", encoding="utf-8") as fp:
    json.dump(items, fp, ensure_ascii=False, indent=4)

with open("attribute_names.json", "w", encoding="utf-8") as fp:
    json.dump(list(attribute_names), fp, ensure_ascii=False, indent=4)

print(len(items))
