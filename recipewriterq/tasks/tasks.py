__author__ = "Tyler Pearson <tdpearson>"

from celery.task import task
from hashlib import sha1, md5
from collections import OrderedDict
from json import dumps
from operator import itemgetter
from uuid import uuid5, NAMESPACE_DNS
import logging
import pandas as pd
import requests
import os

try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse

try:
    from io import StringIO
except ImportError:
    from StringIO import StringIO


#Default base directory
basedir = "/data/web_data/static"
hostname = "https://cc.lib.ou.edu"

#Default manifest file name
bag_manifest_file = "manifest-md5.txt"

logging.basicConfig(level=logging.INFO)

repoUUID = uuid5(NAMESPACE_DNS, 'repository.ou.edu')
repoSHA = sha1(bytes(str(repoUUID), "ASCII")).hexdigest()

# Assert correct generation
assert str(repoUUID) == "eb0ecf41-a457-5220-893a-08b7604b7110"
assert repoSHA == "1639fb25f09f85a3b035bd7a0a62b2a9c7e00c18"


def io_from(path):
    """ returns io object to url contents or file """
    try:
        logging.debug("Reading url or file...")
        if urlparse(path).scheme.lower() in ["http", "https"]:
            logging.debug("detected url")
            return StringIO(requests.get(path).content.decode('utf-8'))
        else:
            logging.debug("detected file")
            return open(path)
    except ConnectionError as err:
        logging.error(err)
    except IOError as err:
        logging.error(err)


def process_csv(inpath):
    """ Returns pandas dataframe from csv bag listing """
    logging.debug("Processing CSV...")
    frame = pd.read_csv(io_from(inpath))[['MMS ID', 'Title', 'File name']]
    has_missing_details = frame[frame.isnull().any(axis=1) == True]
    working_list = frame[frame.isnull().any(axis=1) == False]
    if not has_missing_details.empty:  # we have rows with missing details - log it
        logging.warning("Lines with missing details:\n%s" % has_missing_details)
    logging.debug("Working list:\n%s" % working_list)
    
    return working_list


def get_bag_manifest(bagpath, manifest=bag_manifest_file):
    """ Returns manifest file contents """
    try:
        logging.debug("Locating bag manifest: %s/%s" % (bagpath, manifest))
        return io_from("%s/%s" % (bagpath, manifest)).read()
    except AttributeError as err:
        logging.error(err)


def process_manifest(bagname, bagpath, content):
    """ Returns list with ordered page details """
    if content:
        logging.debug("Processing manifest...")
        split_lines = [line.split("  ") for line in content.split("\n") if line]
        tif_list = [[tif_hash, tif_filename] for tif_hash, tif_filename in split_lines if 'tif' in tif_filename.lower()]
        sorted_tif_list = sorted(tif_list, key=itemgetter(1))

        pages = []

        logging.debug("Processing pages...")
        for index, file_details in enumerate(sorted_tif_list):
            tif_hash, tif_filename = file_details
            page = OrderedDict()
            page['label'] = "Image %s" % str(index + 1)
            page['file'] = "%s/%s" % (bagpath, tif_filename)
            page['md5'] = tif_hash
            page['uuid'] = str(uuid5(repoUUID, "%s/%s" % (bagname, tif_filename)))
            page['exif'] = "%s.exif.txt" % tif_filename.split("/")[1]

            logging.debug(page)
            pages.append(page)

        return pages
    logging.debug("No content in manifest...")


def process_bag(bagpath, mmsid, title, filename):
    logging.info("Processing bag: %s" % filename)
    logging.debug("bagpath: %s" % bagpath)
    logging.debug("mmsid: %s" % mmsid)
    logging.debug("title: %s" % title)
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

    logging.debug("Generated JSON:\n%s" % dumps(meta, indent=4))
    return dumps(meta, indent=4, ensure_ascii=False).encode("UTF-8")


@task()
def generate_recipe_files(csvpath, bagpath):
    """
    Repository recipe generator for Islandora
    args:
      csvpath - path or url to csv file (file must contain MMS ID, Title, and File name details)
      bagpath - base path or url containing bags to process
    """

    task_id = str(generate_recipe_files.request.id)
    # create Result Directory
    resultpath = os.path.join(basedir, 'oulib_tasks/', task_id)

    os.makedirs(resultpath)

    for row in process_csv(csvpath).iterrows():
        mmsid, title, filename = row[1]  # ignore index
        json_file = os.path.join(resultpath, filename + ".json")
        with open(json_file, "wb") as f:
            f.write(process_bag(bagpath, mmsid, title, filename))

    return "{0}/oulib_tasks/{1}".format(hostname, task_id)
