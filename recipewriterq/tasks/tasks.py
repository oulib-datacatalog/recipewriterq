__author__ = "Tyler Pearson <tdpearson>"

from celery.task import task
from collections import OrderedDict
from json import dumps
from uuid import uuid5, NAMESPACE_DNS
import bagit
import logging
import os


#Default base directory
basedir = "/data/web_data/static"
hostname = "https://cc.lib.ou.edu"


logging.basicConfig(level=logging.INFO)

repoUUID = uuid5(NAMESPACE_DNS, 'repository.ou.edu')

# Assert correct generation
assert str(repoUUID) == "eb0ecf41-a457-5220-893a-08b7604b7110"


def process_manifest(bagname, payload, include_exif=False):
    """ Returns list with ordered page details """
    pages = []

    logging.debug("Processing pages...")
    for index, item in enumerate(sorted(payload)):
        filename, hashes = item, payload[item]
        page = OrderedDict()
        page['label'] = "Image {0}".format(str(index + 1))
        page['file'] = "{0}/{1}/{2}".format(hostname, bagname, filename)
        for page_hash in hashes:
            page[page_hash] = hashes[page_hash]
        page['uuid'] = str(uuid5(repoUUID, "{0}/{1}".format(bagname, filename)))
        if include_exif:
            page['exif'] = "{0}.exif.txt".format(filename.split("/")[1])
        logging.debug(page)
        pages.append(page)

    return pages


def generate_recipe(mmsid, title, bagname, payload):
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
    if get_marc_xml(mmsid):
        meta['recipe']['metadata'] = OrderedDict()
        meta['recipe']['metadata']['marcxml'] = bagname + ".xml"

    meta['recipe']['pages'] = process_manifest(bagname, payload)

    logging.debug("Generated JSON:\n{0}".format(dumps(meta, indent=4)))
    return dumps(meta, indent=4, ensure_ascii=False).encode("UTF-8")


def get_marc_xml(mmsid):
    """ Queries Alma with MMS ID to obtain corresponding MARC XML """
    # TODO: write code
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
      title: Title of collection
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
            recipe = generate_recipe(mmsid, title, bagname, payload)
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

    bag_path = "{0}/oulib_tasks/{1}/derivative/".format(basedir, taskid)
    for path in os.listdir(bag_path):
        fullpath = "{0}/{1}".format(bag_path, path)
        try:
            bag = bagit.Bag(fullpath)
        except bagit.BagError:
            bag = bagit.make_bag(fullpath)

        bag.info['External-Description'] = path
        bag.info['External-Identifier'] = 'University of Oklahoma Libraries'

        try:
            bag.save(manifests=update_manifest)
        except IOError as err:
            logging.error(err)
    # point back at task
    return "{0}/oulib_tasks/{1}".format(hostname, taskid)

