import glob

import lxml.etree as ET
from acdh_tei_pyutils.tei import TeiReader

try:
    from analysis.utils import node_to_json
except ModuleNotFoundError:
    pass

items = {}
files = sorted(glob.glob("./data/f*.xml"))
attribute_names = set()
for x in files:
    doc = TeiReader(x)
    print(f"processing {x}")
    for y in doc.any_xpath(".//*[./tei:g]"):
        print(ET.tostring(y))
