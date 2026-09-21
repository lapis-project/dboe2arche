import lxml.etree as ET
from acdh_tei_pyutils.utils import extract_fulltext_with_spacing


def node_to_json(node: ET.Element) -> dict:
    item = {"text": extract_fulltext_with_spacing(node)}
    for attr_name, attr_value in node.attrib.items():
        item[attr_name.split("}")[-1]] = attr_value
    for child in node:
        tag_name = child.tag.split("}")[-1]
        if child.attrib:
            attributes = "__".join(
                f"{attr_name.split('}')[-1]}_{attr_value}"
                for attr_name, attr_value in sorted(child.attrib.items())
            )
            tag_name = f"{tag_name}__{attributes}"
        item.setdefault(tag_name, []).append(extract_fulltext_with_spacing(child))
    return item
