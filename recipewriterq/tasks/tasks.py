__author__ = "Tyler Pearson <tdpearson>"

from celery.task import task
from collections import OrderedDict
from json import dumps
from uuid import uuid5, NAMESPACE_DNS
import xml.etree.cElementTree as ET
import bagit
import logging
import os
import requests

# Default base directory
basedir = "/data/web_data/static"
hostname = "https://cc.lib.ou.edu"

apikeypath = "/code/alma_api_key"

logging.basicConfig(level=logging.INFO)

repoUUID = uuid5(NAMESPACE_DNS, 'repository.ou.edu')

# Assert correct generation
assert str(repoUUID) == "eb0ecf41-a457-5220-893a-08b7604b7110"


def process_manifest(taskid, bagname, payload, include_exif=True):
    """ Returns list with ordered page details """
    pages = []
    logging.debug("Processing pages...")
    for index, item in enumerate(sorted(payload)):
        filename, hashes = item, payload[item]
        page = OrderedDict()
        page['label'] = "Image {0}".format(str(index + 1))
        page['file'] = "{0}/oulib_tasks/{1}/derivative/{2}/{3}".format(hostname, taskid, bagname, filename)
        for page_hash in hashes:
            page[page_hash] = hashes[page_hash]
        page['uuid'] = str(uuid5(repoUUID, "{0}/{1}".format(bagname, filename)))
        if include_exif:
            page['exif'] = "{0}.exif.txt".format(filename.split("/")[1])
        logging.debug(page)
        pages.append(page)

    return pages


def generate_recipe(mmsid, taskid, title, bagname, payload, fullpath):
    """ generates recipe and returns json string """
    logging.info("Processing bag: {0}".format(bagname))
    logging.debug("mmsid: {0}".format(mmsid))
    logging.debug("title: {0}".format(title))
    meta = OrderedDict()
    meta['recipe'] = OrderedDict()
    meta['recipe']['import'] = 'book'
    meta['recipe']['update'] = 'false'
    meta['recipe']['uuid'] = str(uuid5(repoUUID, bagname))
    meta['recipe']['label'] = title
    if get_marc_xml(mmsid, bagname, fullpath):
        meta['recipe']['metadata'] = OrderedDict()
        meta['recipe']['metadata']['marcxml'] = "{0}/oulib_tasks/{1}/derivative/{2}/{2}.xml".format(hostname, taskid, bagname)
    if not title:
        # attempt to set from marc xml
        logging.debug("Getting title from marc file")
        meta['recipe']['label'] = get_title_from_marc(bagname, fullpath)

    meta['recipe']['pages'] = process_manifest(taskid, bagname, payload)

    logging.debug("Generated JSON:\n{0}".format(dumps(meta, indent=4)))
    return dumps(meta, indent=4, ensure_ascii=False).encode("UTF-8")


def get_title_from_marc(bagname, fullpath):
    try:
        tree = ET.parse("{0}/{1}.xml".format(fullpath, bagname))
        root = tree.getroot()
        return root.findall('title')[0].text
    except (IndexError, IOError) as err:
        logging.error(err)
        return None


def get_marc_xml(mmsid, bagname, fullpath):
    """ Queries Alma with MMS ID to obtain corresponding MARC XML """

    url = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{0}?expand=None&apikey={1}"

    try:
        apikey = open(apikeypath).read().strip()
    except IOError:
        apikey = None
        logging.error("Could not load apikey")

    if apikey:
        try:
            response = requests.get(url.format(mmsid, apikey))
            with open("{0}/{1}.xml".format(fullpath, bagname), "wb") as f:
                f.write(response.content)
                return True
        except (IOError, requests.ConnectionError) as err:
            logging.error(err)
    # otherwise
    return False


@task()
def derivative_recipe(taskid, mmsid=None, title=None):
    """
    Generate recipe json file from derivative.

    This requires that the derivative has already been bagged.
    This will add the json file and update the tag-manifest file.

    args:
      taskid: cybercommons generated task id for derivative
      mmsid: MMS ID is needed to obtain MARC XML
      title: Title of collection - If not set, will attempt to fetch from MARC XML
    """

    derivatives = "{0}/oulib_tasks/{1}/derivative/".format(basedir, taskid)
    for path in os.listdir(derivatives):
        fullpath = "{0}/{1}".format(derivatives, path)
        try:
            logging.debug("Accessing: {0}".format(path))
            bag = bagit.Bag(fullpath)
            bagname = bag.info['External-Description']
            payload = bag.payload_entries()
            recipefile = "{0}/{1}.json".format(fullpath, bagname)
            recipe = generate_recipe(mmsid, taskid, title, bagname, payload, fullpath)
            logging.debug("Writing recipe to: {0}".format(recipefile))
            with open(recipefile, "w") as f:
                f.write(recipe)
            bag.save()

        except bagit.BagError:
            logging.debug("Not a bag: {0}".format(path))
            pass
        except IOError as err:
            logging.error(err)
    # point back at task
    return "{0}/oulib_tasks/{1}".format(hostname, taskid)


@task()
def bag_derivatives(taskid, update_manifest=True):
    """
    Generate bag of derivative

    args:
      taskid: cybercommons generated task id for derivative
      update_manifest: boolean to update bag manifest - default is True
    """

    bagpath = "{0}/oulib_tasks/{1}/derivative/".format(basedir, taskid)
    for bagname in os.listdir(bagpath):
        fullpath = "{0}/{1}".format(bagpath, bagname)
        try:
            bag = bagit.Bag(fullpath)
        except bagit.BagError:
            bag = bagit.make_bag(fullpath)

        bag.info['External-Description'] = bagname
        bag.info['External-Identifier'] = 'University of Oklahoma Libraries'

        try:
            bag.save(manifests=update_manifest)
        except IOError as err:
            logging.error(err)
    # point back at task
    return "{0}/oulib_tasks/{1}".format(hostname, taskid)

