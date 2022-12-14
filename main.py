import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
import numpy as np
import re
import requests
from pathlib import Path
import json
from collections import OrderedDict


def ordered(d, desired_key_order):
    return OrderedDict([(key, d[key]) for key in desired_key_order])


# Disired key order for .json file
desired_key_order = ("@context", "@id", "@type", "label", "metadata", "structures", "sequences")

df_kalktekening = pd.read_csv('input/kalktekeningen-compleet.csv', delimiter=';')
df_GMS_meta = pd.read_excel('input/scans_GMS_metadata_1.4.xlsx', header=0)
df_gebouw = pd.read_excel('input/Overzicht uitgegeven gebouwnummers 20-06-2017.xls', header=2)
df_gebouw_ontbreek = pd.read_table('additional_buildings.txt',
                                   delimiter=';', header=None,
                                   comment='#', names=['Folder', 'Building'])

# Base urls
dlcs_base = "https://dlc.services/iiif-resource/7/string1string2string3/{}/{}"
base_ref_id = "https://raw.githubusercontent.com/tu-delft-library/kalktekeningen-cre/main/manifests/kokers/{}.json"

koker_groups = df_kalktekening.groupby('Reference2').indices

df_miss_meta = pd.DataFrame(columns=['uuid', 'url', 'filename'])
df_miss_building = pd.DataFrame(columns=['uuid', 'url', 'filename', 'folder', 'building'])

koker_keys = df_GMS_meta.keys()
geb_keys = df_gebouw.keys()

for koker in koker_keys:
    df_kalktekening[koker] = np.nan

for geb in geb_keys:
    df_kalktekening[geb] = np.nan

koker_collection = []

for i, key in enumerate(koker_groups.keys()):
    gms_dat = df_GMS_meta[df_GMS_meta['INV.NRkoker'] == key]
    ref1 = 'kalktekeningen'
    ref2 = key

    manifest_url = dlcs_base.format(ref1, ref2)
    json_manifest = requests.get(manifest_url).json()
    koker_dat = pd.DataFrame()
    gebouw_dat = pd.DataFrame()
    manifests = []

    canvases = json_manifest["sequences"][0]["canvases"]

    for j, canvas in enumerate(canvases):
        id = canvas["@id"].split('=')[-1]

        image = df_kalktekening[df_kalktekening["NumberReference1"] == np.int_(id)]

        # for j, im_nr in enumerate(koker_groups[key]):
        #     image = df_kalktekening.loc[im_nr]
        url = image['Origin'].values[0]
        filename = url.split('/')[-1].rstrip('.jpg').replace('%20', ' ')

        adjustments = [filename,
                       filename.replace('%23', '#'),
                       filename + '.',
                       filename.strip(' '),
                       filename.replace('(', '').replace(')', ''),
                       np.int_(filename.replace('.', '')) if filename.replace('.', '').isdigit() else '---']

        for adj_name in adjustments:
            df_koker_new = gms_dat[gms_dat['TEKENINGNUMMER'] == adj_name]
            koker_dat = koker_dat.append(df_koker_new)

            if not df_koker_new.empty:
                break

        if df_koker_new.empty:
            print('cannot find {} in {}'.format(filename, key))
            df_miss_meta = df_miss_meta.append({'uuid': image['ID'],
                                                'url': url,
                                                'filename': filename}, ignore_index=True)
        else:
            df_kalktekening.loc[image.index, koker_keys] = df_koker_new.iloc[0].values
            df_geb_new = df_gebouw[df_gebouw['gb nr'] == koker_dat['Gebouw'].values[0]]
            gebouw_dat = gebouw_dat.append(df_geb_new)

            # Check if building could be found
            if df_geb_new.empty:
                building = df_gebouw_ontbreek[df_gebouw_ontbreek['Folder'] == key]['Building'].values[0]
                gebouw_dat = gebouw_dat.append({'meest gangbare naam ': building}, ignore_index=True)

                df_kalktekening.loc[image.index, "meest gangbare naam "] = building
            else:
                df_kalktekening.loc[image.index, geb_keys] = df_geb_new.values

                # print('Cannot directly find building {}'.format(koker_dat['Gebouw'].values[0]))
                # df_miss_building = df_miss_building.append({'uuid': image['ID'],
                #                                             'url': url,
                #                                             'filename': filename,
                #                                             'folder': ref2,
                #                                             'building': koker_dat['Gebouw'].values[0]},
                #                                            ignore_index=True)

        json_manifest["sequences"][0]["canvases"][j]["label"] = koker_dat.iloc[-1]['OMSCHRIJVING'].lower().capitalize()
        # ref_id = base_ref_id.format(ref2)+".json"
        # mani = {"@id": ref_id,
        #         "label": koker_dat.iloc[-1]['OMSCHRIJVING'],
        #         "@type": "sc:Manifest"}
        # manifests.append(mani)

    if gebouw_dat.empty:
        gebouw_naam = koker_dat['Gebouw'].values[0]
    else:
        gebouw_naam = gebouw_dat.iloc[0]['meest gangbare naam ']

    # Input meta data for collection manifest
    meta = [{
        "label": "Titel",
        "value": koker_dat.iloc[0]['Naam koker']
    },
        {
            "label": "Koker",
            "value": ref2
        }]
    if not str(koker_dat.iloc[0]['vertaling naam']) == 'nan':
        meta.append({
            "label": "Vertaling naam",
            "value": str(koker_dat.iloc[0]['vertaling naam'])
        })
    meta.append({
        "label": "Gebouw",
        "value": gebouw_naam
    })

    if not str(koker_dat.iloc[0]['Vleugel']) == 'nan':
        meta.append({
            "label": "Vleugel",
            "value": str(koker_dat.iloc[0]['Vleugel'])
        })

    json_manifest['metadata'] = meta

    # Insert data into collection manifest
    # json_out = {"label": ref2,
    #             "metadata": meta,
    #             "@id": base_ref_id.format(ref2, ref2),
    #             "@type": "sc:Collection",
    #             "@context": "http://iiif.io/api/presentation/2/context.json",
    #             "manifests": manifests}

    koker_id = base_ref_id.format(ref2)

    koker_collection.append({
        "@id": koker_id,
        "@type": "sc:Collection",
        "label": koker_dat.iloc[0]['Naam koker']
    })
    # koker_collection[i] = {
    #     "@id": koker_id,
    #     "@type": "sc:Collection",
    #     "label": koker_dat.iloc[0]['Naam koker']
    # }

    json_year = json.dumps(json_manifest, indent=8)
    Path("manifests/kokers").mkdir(parents=True, exist_ok=True)
    with open("manifests/kokers/" + ref2 + ".json", "w") as outfile:
        outfile.write(json_year)

build_collection = []
building_groups = df_kalktekening.groupby(['meest gangbare naam ']).indices

for i, key in enumerate(building_groups.keys()):
    init_build = df_kalktekening.loc[building_groups[key][0]]
    build_group = df_kalktekening.loc[building_groups[key]]

    filename = key.replace(" ", "_").replace("/", "")
    filename = filename.lstrip("_").replace("__", "_")

    meta = [{
        "label": "Building",
        "value": key
    }]
    if not str(init_build['vertaling naam']) == 'nan':
        meta.append({
            "label": "Vertaling naam",
            "value": str(init_build['vertaling naam'])
        })
    meta.append({
        "label": "Gebouw",
        "value": gebouw_naam
    })

    if not str(init_build['Vleugel']) == 'nan':
        meta.append({
            "label": "Vleugel",
            "value": str(init_build['Vleugel'])
        })

    build_id = "https://raw.githubusercontent.com/tu-delft-library/kalktekeningen-cre/main/manifests/gebouwen/{}.json".format(filename)
    build_manifest = {
        "@context": "http://iiif.io/api/presentation/2/context.json",
        "@id": build_id,
        "@type": "sc:Collection",
        "label": key,
        "viewingHint": "top",
        "description": "",
        "attribution": "TU Delft Library",
        "collections": []
    }

    build_collection.append({
        "@id": build_id,
        "@type": "sc:Manifest",
        "label": key
    })
    # build_collection[i] = {
    #     "@id": build_id,
    #     "@type": "sc:Collection",
    #     "label": key
    # }

    build_koker_group = build_group.groupby(['Reference2']).indices

    collection = []
    for j, koker in enumerate(build_koker_group.keys()):
        koker_mani_location = base_ref_id.format(koker)
        collection.append({
            "@id": koker_mani_location,
            "@type": "sc:Manifest",
            "label": koker
        })
        # collection[j] = {
        #     "@id": koker_mani_location,
        #     "@type": "sc:Collection",
        #     "label": koker
        # }
    build_manifest["collections"] = collection
    json_year = json.dumps(build_manifest, indent=8)
    Path("manifests/gebouwen").mkdir(parents=True, exist_ok=True)
    with open("manifests/gebouwen/" + filename + ".json", "w") as outfile:
        outfile.write(json_year)

gebouwen_manifest = {
    "@context": "http://iiif.io/api/presentation/2/context.json",
    "@id": "https://raw.githubusercontent.com/tu-delft-library/kalktekeningen-cre/main/manifests/gebouwen.json",
    "@type": "sc:Collection",
    "label": "Gebouwen",
    "viewingHint": "top",
    "description": "",
    "attribution": "TU Delft Library",
    "collections": build_collection
}
json_out = json.dumps(gebouwen_manifest, indent=8)
with open("manifests/gebouwen.json", "w") as outfile:
    outfile.write(json_out)


kokers_manifest = {
    "@context": "http://iiif.io/api/presentation/2/context.json",
    "@id": "https://raw.githubusercontent.com/tu-delft-library/kalktekeningen-cre/main/manifests/kokers.json",
    "@type": "sc:Collection",
    "label": "Kokers",
    "viewingHint": "top",
    "description": "",
    "attribution": "TU Delft Library",
    "collections": koker_collection
}
json_out = json.dumps(kokers_manifest, indent=8)
with open("manifests/kokers.json", "w") as outfile:
    outfile.write(json_out)

