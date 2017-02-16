__author__ = "Tyler Pearson <tdpearson>"

from hashlib import sha1, md5
from collections import OrderedDict
from json import dumps
from operator import itemgetter
from uuid import uuid5, NAMESPACE_DNS
import logging
import pandas as pd
import requests

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


repoUUID = uuid5(NAMESPACE_DNS, 'repository.ou.edu')
repoSHA = sha1(bytes(str(repoUUID), "ASCII")).hexdigest()

# Assert correct generation
assert str(repoUUID) == "eb0ecf41-a457-5220-893a-08b7604b7110"
assert repoSHA == "1639fb25f09f85a3b035bd7a0a62b2a9c7e00c18"


logging.basicConfig(level=logging.INFO)

bag_manifest_file = "manifest-md5.txt"  # stores md5 checksums of bagged data
tag_manifest_file = "tagmanifest-md5.txt"  # stores md5 checksums of bagit generated data


def process_csv(inpath):
    """ Returns pandas dataframe from csv bag listing """
    frame = pd.read_csv(inpath)[['MMS ID', 'Title', 'File name']]
    has_missing_details = frame[frame.isnull().any(axis=1) == True]
    working_list = frame[frame.isnull().any(axis=1) == False]
    if not has_missing_details.empty:  # we have rows with missing details - log it
        logging.warning("Lines with missing details:\n%s" % has_missing_details)
    logging.debug("Working list:\n%s" % working_list)
    
    return working_list


def get_bag_manifest(bagpath, manifest=bag_manifest_file):
    """ Returns manifest file contents """
    try:
        logging.debug("Getting bag manifest using path: %s" % bagpath)
        if urlparse(bagpath).scheme.lower() in ["http", "https"]:
            logging.debug("Using requests")
            return requests.get("%s/%s" % (bagpath, manifest)).content
        else:
            logging.debug("Using local file path")
            return open("%s/%s" % (bagpath, manifest)).read()
    except ConnectionError as err:
        logging.error(err)
    except IOError as err:
        logging.error(err)


def process_manifest(bagname, bagpath, content):
    """ Returns list of OrderedDict page details """
    if content:
        split_lines = [line.split("  ") for line in content.split("\n") if line]
        tif_list = [[tif_hash, tif_filename] for tif_hash, tif_filename in split_lines if 'tif' in tif_filename.lower()]
        sorted_tif_list = sorted(tif_list, key=itemgetter(1))

        pages = []

        logging.info("Processing %s " % bagname)
        for index, file_details in enumerate(sorted_tif_list):
            tif_hash, tif_filename = file_details
            page = OrderedDict()
            page['label'] = "Image %s" % str(index + 1)
            page['file'] = "%s/%s" % (bagpath, tif_filename)
            page['md5'] = tif_hash
            page['uuid'] = str(uuid5(repoUUID, "%s/%s" % (bagname, tif_filename)))
            page['exif'] = "%s.exit.txt" % tif_filename.split("/")[1]

            logging.debug(page)
            pages.append(page)

        return pages


def process_bag(bagpath, mmsid, title, filename):
    meta = OrderedDict()
    meta['recipe'] = OrderedDict()
    meta['recipe']['import'] = 'book'
    meta['recipe']['update'] = 'false'
    meta['recipe']['uuid'] = str(uuid5(repoUUID, filename))
    meta['recipe']['label'] = title
    meta['recipe']['metadata'] = OrderedDict()
    # the xml file is populated with data provided from http://52.0.88.11 (get request with bib_id = mmsid)
    meta['recipe']['metadata']['marcxml'] = filename + ".xml"

    manifest = get_bag_manifest("%s/%s" % (bagpath, filename))
    meta['recipe']['pages'] = process_manifest(filename, bagpath, manifest)

    logging.debug(dumps(meta, indent=4))
    return dumps(meta, indent=4, ensure_ascii=False).encode("UTF-8")


def update_tag_manifest(tagpath, targetfile):
    tagcontents = open(tagpath).read()
    if targetfile not in tagcontents:
        md5sum = md5(open(targetfile, "rb").read()).hexdigest()
        with open(tagpath, "a") as f:
            f.write("\n%s  %s" % (md5sum, targetfile))
    else:
        pass  # TODO: handle if targetfile already exists in tagcontents


if __name__ == "__main__":
    cookbook = r"./3Z9M2.csv"
    bagpath = r"."
    outpath = "./json"

    for row in process_csv(cookbook).iterrows():
        mmsid, title, filename = row[1]  # ignore index
        json_file = "%s/%s.json" % (outpath, filename)
        with open(json_file, "wb") as f:
            f.write(process_bag(bagpath, mmsid, title, filename))
        update_tag_manifest("%s/%s/tagmanifest-md5.txt" % (bagpath, filename), json_file)
