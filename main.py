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

# Base urls
dlcs_base = "https://dlc.services/iiif-resource/7/string1string2string3/{}/{}"
base_ref_id = "https://tu-delft-library.github.io/kalktekeningen-cre/manifests/koker/{}"

koker_groups = df_kalktekening.groupby('Reference2').indices

df_miss_meta = pd.DataFrame(columns=['uuid', 'url', 'filename'])
df_miss_building = pd.DataFrame(columns=['uuid', 'url', 'filename', 'folder' , 'building'])

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
        filename = url.split('/')[-1].strip('.jpg').replace('%20', ' ')

        adjustments = [filename,
                       filename.replace('%23', '#'),
                       filename + '.',
                       filename.strip(' '),
                       filename.strip('()'),
                       np.int_(filename.replace('.', '')) if filename.replace('.', '').isdigit() else '---']

        for adj_name in adjustments:
            koker_dat = koker_dat.append(gms_dat[gms_dat['TEKENINGNUMMER'] == adj_name])
            if not koker_dat.empty:
                break

        if koker_dat.empty:
            print('cannot find {} in {}'.format(filename, key))
            df_miss_meta = df_miss_meta.append({'uuid': image['ID'],
                                                'url': url,
                                                'filename': filename}, ignore_index=True)
        else:
            gebouw_dat = gebouw_dat.append(df_gebouw[
                df_gebouw['gb nr'] == koker_dat['Gebouw'].values[0]])

            if gebouw_dat.empty:
                print('Cannot directly find building {}'.format(koker_dat['Gebouw'].values[0]))
                df_miss_building = df_miss_building.append({'uuid': image['ID'],
                                                            'url': url,
                                                            'filename': filename,
                                                            'folder': ref2,
                                                            'building': koker_dat['Gebouw'].values[0]},
                                                           ignore_index=True)

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
                "value": ref2
            },
            {
                "label": "Koker",
                "value": koker_dat.iloc[0]['Naam koker']
            },
            {
                "label": "Vertaling naam",
                "value": str(koker_dat.iloc[0]['vertaling naam'])
            },
            {
                "label": "Gebouw",
                "value": gebouw_naam
            },
            {
                "label": "Vleugel",
                "value": str(koker_dat.iloc[0]['Vleugel'])
            }]

    json_manifest['metadata'] = meta


    # Insert data into collection manifest
    # json_out = {"label": ref2,
    #             "metadata": meta,
    #             "@id": base_ref_id.format(ref2, ref2),
    #             "@type": "sc:Collection",
    #             "@context": "http://iiif.io/api/presentation/2/context.json",
    #             "manifests": manifests}

    json_year = json.dumps(json_manifest, indent=8)
    Path("manifests/koker").mkdir(parents=True, exist_ok=True)
    with open("manifests/koker/" + ref2 + ".json", "w") as outfile:
        outfile.write(json_year)
