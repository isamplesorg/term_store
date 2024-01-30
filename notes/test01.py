import dataclasses
import json
import os.path
import uuid

import linkml_runtime.loaders
import linkml_runtime.utils.yamlutils
import isamples_core.model

class GUID:
    pass

class Relation:
    pass

class Term:
    pass

def model_components(obj: linkml_runtime.utils.yamlutils.YAMLRoot, pid=None):
    components = []
    cn = obj.__class__.__name__
    uid = uuid.uuid4().urn
    c = {
        "_id": uid,
        "ttype": cn,
        "props": {},
        "pid": pid,
        "cid": [],
    }
    components.append(c)
    field_names = [f.name for f in dataclasses.fields(obj)]
    for f in field_names:
        v = getattr(obj, f)
        if isinstance(v, list):
            c["props"][f] = []
            for vi in v:
                if isinstance(vi, linkml_runtime.utils.yamlutils.YAMLRoot):
                    components += model_components(vi, c["_id"])
                else:
                    c["props"][f].append(vi)
        else:
            if isinstance(v, linkml_runtime.utils.yamlutils.YAMLRoot):
                components += model_components(v, c["_id"])
            else:
               c["props"][f] = v
    for e in components:
        if e["pid"] == c["_id"]:
            c["cid"].append(e["_id"])
    return components


def test_create():
    sample = isamples_core.model.PhysicalSampleRecord(
        sample_identifier = "foo",
        label = "foo label"
    )
    print(sample.model_dump_json(indent=2))


def test_load(src):
    sample = linkml_runtime.loaders.json_loader.load(src, target_class=isamples_core.model.PhysicalSampleRecord)
    c = model_components(sample)
    print(json.dumps(c, indent=2))


def main():
    #test_create()
    eg_dir = "/Users/vieglais/Documents/Projects/isamples/source/model/metadata/examples/GEOME/test1.0Valid"
    src = os.path.join(eg_dir, "ark-21547-AvL2C02_201705281001-v1.json")
    test_load(src)

if __name__ == "__main__":
    main()