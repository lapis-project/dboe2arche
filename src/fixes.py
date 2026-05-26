import glob

from tqdm import tqdm

files = sorted(glob.glob("./data/*.xml"))

replace_with = """<?xml version="1.0" encoding="UTF-8"?>
<?xml-model href="http://www.tei-c.org/release/xml/tei/custom/schema/relaxng/tei_all.rng" type="application/xml" schematypens="http://relaxng.org/ns/structure/1.0"?>
<?xml-model href="http://www.tei-c.org/release/xml/tei/custom/schema/relaxng/tei_all.rng" type="application/xml"
	schematypens="http://purl.oclc.org/dsdl/schematron"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0">
"""

for x in tqdm(files):
    with open(x, "r", encoding="utf-8") as fp:
        data = fp.read()

    if not data:
        continue

    if not data.startswith('<?xml version="1.0"'):
        first_newline = data.find("\n")
        if first_newline == -1:
            data = replace_with
        else:
            data = replace_with + data[first_newline + 1 :]

    data = data.replace("<listPlace/>", "")

    with open(x, "w", encoding="utf-8") as fp:
        fp.write(data)
