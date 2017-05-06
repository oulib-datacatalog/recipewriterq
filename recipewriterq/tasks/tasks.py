__author__ = "Tyler Pearson <tdpearson>"

from celery.task import task
from collections import OrderedDict
from glob import iglob
from json import dumps, loads
from shutil import rmtree
from uuid import uuid5, NAMESPACE_DNS
import xml.etree.cElementTree as ET
import bagit
import boto3
import datetime
import logging
import os
import requests

# Default base directory
basedir = "/data/web_data/static"
base_url = "https://cc.lib.ou.edu"
api_url = "{0}/api".format(base_url)
catalog_url = "{0}/catalog/data/catalog/digital_objects/.json".format(api_url)
ou_derivative_bag_url = "https://bag.ou.edu/derivative"
recipe_url = ou_derivative_bag_url + "/{0}/{1}/{2}.json"  # bagname, param string, lowercase bagname
search_url = "{0}?query={{\"filter\": {{\"bag\": \"{1}\"}}}}"

apikeypath = "/code/alma_api_key"
cctokenfile = "/code/cybercom_token"


logging.basicConfig(level=logging.INFO)

repoUUID = uuid5(NAMESPACE_DNS, 'repository.ou.edu')

# Assert correct generation
assert str(repoUUID) == "eb0ecf41-a457-5220-893a-08b7604b7110"


def process_manifest(taskid, bagname, payload, formatparams=None, include_exif=True):
    """ Returns list with ordered page details """
    pages = []
    logging.debug("Processing pages...")
    for index, item in enumerate(sorted(payload)):
        filename, hashes = item, payload[item]
        page = OrderedDict()
        page['label'] = "Image {0}".format(str(index + 1))
        if formatparams:
            page['file'] = "{0}/{1}/{2}/{3}".format(ou_derivative_bag_url, bagname, formatparams, filename)
        else:
            page['file'] = "{0}/{1}/{2}".format(ou_derivative_bag_url, bagname, filename)

        for page_hash in hashes:
            page[page_hash] = hashes[page_hash]
        page['uuid'] = str(uuid5(repoUUID, "{0}/{1}".format(bagname, filename)))
        if include_exif:
            page['exif'] = "{0}.exif.txt".format(filename.split("/")[1])
        logging.debug(page)
        pages.append(page)

    return pages


def generate_recipe(mmsid, taskid, title, bagname, payload, fullpath, formatparams=None):
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

    bib = get_bib_record(mmsid)
    if get_marc_xml(mmsid, bagname, fullpath, bib):
        meta['recipe']['metadata'] = OrderedDict()
        if formatparams:
            meta['recipe']['metadata']['marcxml'] = "{0}/{1}/{2}/marc.xml".format(ou_derivative_bag_url, bagname, formatparams) 
        
        else:
            meta['recipe']['metadata']['marcxml'] = "{0}/{1}/marc.xml".format(ou_derivative_bag_url, bagname)

    if not title:
        # attempt to set from marc xml
        logging.debug("Getting title from marc file")
        meta['recipe']['label'] = get_title_from_bib(bib)

    meta['recipe']['pages'] = process_manifest(taskid, bagname, payload, formatparams)

    logging.debug("Generated JSON:\n{0}".format(dumps(meta, indent=4)))
    return dumps(meta, indent=4, ensure_ascii=False).encode("UTF-8")


def get_bib_record(mmsid):
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
            return response.content
        except requests.ConnectionError as err:
            logging.error(err)
            return None


def get_title_from_bib(xml):
    try:
        tree = ET.fromstring(xml)
        return tree.find('title').text
    except IndexError as err:
        logging.error(err)
        return None


def get_marc_xml(mmsid, bagname, fullpath, bibxml):
    """ Gets MARC21 record from bib xml """

    record = ET.fromstring(bibxml).find("record")
    record.attrib['xmlns'] = "http://www.loc.gov/MARC21/slim"
    if not record.find(".//*[@tag='001']"):  # add if missing id
        controlfield = ET.Element("controlfield", tag="001")
        controlfield.text = mmsid
        record.insert(1, controlfield)
    marc21 = ET.ElementTree(record)
    try:
        marc21.write("{0}/marc.xml".format(fullpath), encoding='utf-8', xml_declaration=True)
        return True
    except IOError as err:
        logging.error(err)
        return False


def searchcatalog(bag):
    resp = requests.get(search_url.format(catalog_url, bag))
    catalogitems = loads(resp.text)
    if catalogitems['count']:
        return catalogitems['results'][0]


def listpagefiles(bag, paramstring):
    s3_bucket='ul-bagit'
    s3_destination='derivative'
    s3 = boto3.resource('s3')
    filename = "{0}.json".format(bag).lower()
    s3_key = "{0}/{1}/{2}/{3}".format(s3_destination, bag, paramstring, filename)
    recipe_obj = s3.object(s3_bucket, s3_key)
    recipe_json = recipe_obj.get()['Body'].read()
    recipe = loads(recipe_json)
    #resp = requests.get(recipe_url.format(bag, paramstring, bag.lower()))
    #recipe = loads(resp.text)
    return [page['file'] for page in recipe['recipe']['pages']]


@task()
def updatecatalog(bag, paramstring):
    """
    Update Bag in Data Catalog with derivative location
    
    args:
      bag (string); Name of bag to update data catalog entry
      paramstring (string);  Parameter settings of derivative (e.x. "jpeg_040_antialias")
    """

    """
    Example derivative record structure:
    {"jpg_040_antialias": {"recipe": <url string to to recipe file>,
                           "datetime": <timestamp of derivative>,
                           "pages": [<list of page urls>]
                           }
     }
    """
    catalogitem = searchcatalog(bag)
    if catalogitem = None:
        return False  # this bag does not have a catalog entry
    
    if paramstring not in catalogitem["derivatives"]:
        catalogitem["derivatives"][paramstring] = {}
    catalogitem["derivatives"][paramstring]["recipe"] = recipe_url.format(bag, paramstring, bag.lower())
    catalogitem["derivatives"][paramstring]["datetime"] = datetime.datetime.utcnow().isoformat()
    catalogitem["derivatives"][paramstring]["pages"] = listpagefiles(bag, paramstring)
    
    token = open(cctokenfile).read().strip()
    headers = {"Content-Type": "application/json", "Authorization": "Token {0}".format(token)}
    req = requests.post(catalog_url, data=dumps(catalogitem), headers=headers)
    req.raise_for_status()
    return True


@task()
def derivative_recipe(taskid, mmsid=None, title=None, formatparams=None):
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
            recipe = generate_recipe(mmsid, taskid, title, bagname, payload, fullpath, formatparams)
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
    return "{0}/oulib_tasks/{1}".format(base_url, taskid)


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
    return "{0}/oulib_tasks/{1}".format(base_url, taskid)


@task()
def process_derivative(derivative_args, mmsid=None, rmlocal=True):
    """
    This task is called as part of the loadbook process. You should not run this directly.

    args:
      derivative_args: results from the derivative_generation task
      mmsid: mmsid of item to load
      rmlocal: boolean indicating to remove local derivative bag after loading to s3 - default is True

    Returns:
      List of urls to recipe files
    """

    s3_bucket='ul-bagit'
    s3_destination='derivative'
    
    taskid = derivative_args.get('task_id')
    s3_bags = derivative_args.get('s3_bags')
    bags = s3_bags if isinstance(s3_bags, list) or s3_bags is None else [s3_bags]  # ensure bags is list or None
    formatparams = derivative_args.get('format_parameters')
    
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_bucket)

    if taskid and bags:
        # generate meta and bag derivatives
        bag_derivatives(taskid)
        derivative_recipe(taskid, mmsid, formatparams=formatparams)
        for bag in bags:
            # move derivative bag into s3
             bagpath = "{0}/oulib_tasks/{1}/derivative/{2}".format(basedir, taskid, bag)
             logging.info("Accessing bag at: {0}".format(bagpath))
             for filepath in iglob("{0}/*.*".format(bagpath)):
                 filename = filepath.split('/')[-1].lower()
                 s3_key = "{0}/{1}/{2}/{3}".format(s3_destination, bag, formatparams, filename)
                 logging.info("Saving {0} to {1}".format(filename, s3_key))
                 s3.meta.client.upload_file(filepath, bucket.name, s3_key)
             for filepath in iglob("{0}/data/*.*".format(bagpath)):
                 filename = filepath.split('/')[-1].lower()
                 s3_key = "{0}/{1}/{2}/data/{3}".format(s3_destination, bag, formatparams, filename)
                 logging.info("Saving {0} to {1}".format(filename, s3_key))
                 s3.meta.client.upload_file(filepath, bucket.name, s3_key)
             updatecatalog(bag, formatparams)
             # remove derivative bag from local system
             if rmlocal:
                 rmtree("{0}/oulib_tasks/{1}".format(basedir, taskid))

        return ["{0}/{1}/{2}/{3}.json".format(ou_derivative_bag_url, bag, formatparams, bag.lower()) for bag in bags]

