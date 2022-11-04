#!/usr/bin/python3
import os.path
import os
import json
import time
from datetime import datetime
import argparse
import ssl
import urllib3
import shutil
import hashlib
import base64
import threading
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import uuid
import socket
import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
from prometheus_client import Counter, Gauge, start_http_server
from prometheus_client import Summary, REGISTRY, PROCESS_COLLECTOR, PLATFORM_COLLECTOR


# Args
parser = argparse.ArgumentParser(
     description='Download data input dir',
     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--inputDir', default="",
                    type=str, help='input dir for download')
parser.add_argument('--config', default="",
                    type=str, help='config file name')
args = parser.parse_args()

if args.config == "":
    print("Please use --config and config file name as argument")
    config_filename = ""
    configFile = "missing"
else:
    config_filename = args.config
    configFile = os.path.basename(config_filename)

configFile = configFile.replace(".json", "")
configFile = configFile.replace(".txt", "")

if args.inputDir == "":
    inputDir = ""
else:
    inputDir = args.inputDir


# functions
def init_log(logFile, logLevel, loggerName):
    global LOG
    handlers = [RotatingFileHandler(filename=logFile,
                mode='a',
                maxBytes=512000,
                backupCount=2)]
    logging.basicConfig(handlers=handlers,
                        level=logLevel,
                        format='%(levelname)s %(asctime)s %(message)s',
                        datefmt='%Y%m%dT%H:%M:%S')
    LOG = logging.getLogger(loggerName)


def init_counter(thisDay):
    global msg_counter_days, monToPub, monToDoJson, monNotCore, monErrors, monOpendata, monDownloads, monWithContent, monDuplicate
    if thisDay not in msg_counter_days.keys():
        msg_counter_days.update({thisDay:0})
        if monitor_metrics == "True":
            for country in metric_countries:
                monToPub.labels(country).set(0)
                monToDoJson.labels(country).set(0)
                monNotCore.labels(country).set(0)
                monErrors.labels(country).set(0)
                monOpendata.labels(country).set(0)
                monDownloads.labels(country).set(0)
                monWithContent.labels(country).set(0)
                monDuplicate.labels(country).set(0)


def init_metric_label(country):
    if monitor_metrics == "True":
        monToPub.labels(country).set(0)
        monToDoJson.labels(country).set(0)
        monNotCore.labels(country).set(0)
        monErrors.labels(country).set(0)
        monOpendata.labels(country).set(0)
        monDownloads.labels(country).set(0)
        monWithContent.labels(country).set(0)
        monDuplicate.labels(country).set(0)

def sameIntegrity(myInfoFile, myIntegrity):
    global sameContent
    if os.path.exists(myInfoFile):
        with open(myInfoFile, 'r') as attrFile:
            for line in attrFile:
                if "None" in line:
                    old_sum = "None"
                else:
                    if "\n" in line:
                        old_sum = line.rstrip("\n")
                    else:
                        old_sum = line
        if old_sum == myIntegrity:
            sameContent = True
        else:
            sameContent = False


def move_json2error(todoJson):
    global errorDir
    myJsonToDo = os.path.basename(todoJson)
    myJsonToDo = myJsonToDo[1:]
    errorFile = os.path.join(errorDir, myJsonToDo)
    LOG.debug(" - move_json2error: errorFile is "
              + str(errorFile))
    src_exists = os.path.exists(todoJson)
    destDir_exists = os.path.exists(os.path.dirname(errorFile))
    if not destDir_exists:
        LOG.error(" - missing error directory: "
                  + str(os.path.dirname(errorFile)))
        os.makedirs(os.path.dirname(errorFile))
    if src_exists:
        shutil.move(todoJson, errorFile)
    else:
        LOG.error(" - missing toDo JSON: " + str(todoJson))
    LOG.error(" - download error occured for json ( "
              + str(todoJson) + " ), move json to "
              + str(errorFile))


def modifyAndAddToPub(data_id, downloadFile, file_topic, topic_country, topic_publisher):
    global pub_connected_flag, monitor_metrics, monErrors, monToPub
    http = urllib3.PoolManager()
    if "/" in data_id:
        filename_data_id = data_id.replace("/", dataId_replace)
    else:
        filename_data_id = data_id

    # read org_msg
    if data_id != "":
        if msg_store != "":
            msgStore_path = os.path.dirname(msg_store)
            if os.path.exists(msgStore_path):
                msgStore_file = os.path.join(
                                             msgStore_path,
                                             filename_data_id)
                if os.path.exists(msgStore_file):
                    with open(msgStore_file, 'r') as msgStore_msg:
                        msgStore_msg = msgStore_msg.read()
                    myMSG = json.loads(msgStore_msg)
                    LOG.debug(json.dumps(msgStore_msg, indent=4))
                else:
                    msgStore_msg = None
                    LOG.error(" - missing msg_store message: "
                              + str(msgStore_file))
            else:
                LOG.error(" - missing msg_store directory, value is: "
                          + str(msgStore_path))
        else:
            LOG.error(" - empty value for msg_store in config file")
    else:
        LOG.error(" - data_id is empty")
    # get fullURL
    if not pub_URLbase.endswith("/"):
        fullURL = pub_URLbase + "/"
    else:
        fullURL = pub_URLbase
    fullURL = fullURL + topic_publisher
    downloaded_filename = os.path.basename(downloadFile)
    fullURL = fullURL + "/" + downloaded_filename
    LOG.debug(" - fullURL whether transmitted: "
             + str(fullURL))
    # modify org_msg links to opendata links
    new_msg_id = str(uuid.uuid1())
    myMSG["id"] = new_msg_id
    cache_pub_now = time.time()
    cache_pub_nsec = ('%.9g' % (cache_pub_now % 1))[1:]
    cache_pub_datestamp = time.strftime(
            "%Y-%m-%dT%H:%M:%S",
                         time.gmtime(cache_pub_now)) + cache_pub_nsec
    gcTime = cache_pub_datestamp + "Z"
    if "pubtime" in myMSG["properties"].keys():
        myMSG["properties"]["pubtime"] = gcTime
    else:
        if "pub_datetime" in myMSG["properties"].keys():
            LOG.warning(" - pub_datetime should be pubtime, updated it")
            del myMSG["properties"]["pub_datetime"]
            myMSG["properties"]["pubtime"] = gcTime
        if ("publication_datetime" in myMSG["properties"].keys()):
            LOG.warning(" - publication_datetime should be pubtime, updated it")
            del myMSG["properties"]["publication_datetime"]
            myMSG["properties"]["pubtime"] = gcTime
        if "pubTime" in myMSG["properties"].keys():
            LOG.warning(" - pubTime should be pubtime, updated it")
            del myMSG["properties"]["pubTime"]
            myMSG["properties"]["pubtime"] = gcTime

    msgStore_url = ""
    if "links" in myMSG["properties"].keys():
        urlList = myMSG["properties"]["links"]
    else:
        if "links" in myMSG.keys():
            urlList = myMSG["links"]
        else:
            LOG.error(" - no links in message: "
                        + str(json.dumps(
                              myMSG, indent=4)))
            urlList = None
    if urlList is not None:
        for item in urlList:
            if item["rel"] == "canonical":
                msgStore_url = item["href"]
        if msgStore_url == "":
            LOG.info(" - no canonical href in links,"
                    + " use first href value instead")
            msgStore_url = urlList[0]["href"]
        if msgStore_url == "":
            LOG.error(" - no links in message: "
                  + str(json.dumps(myMSG, indent=4)))
    else:
        LOG.error(" - modifyMSG - urlList is None")
        if monitor_metrics == "True":
            monErrors.labels("ALL").inc()
            monErrors.labels(topic_country).inc()
    myMSG["links"][0]["href"] = fullURL
    # other value for cache links?
    myMSG["links"][0]["rel"] = "canonical"
    if "4gc/origin/" in file_topic:
        file_topic = file_topic.replace(
                        "4gc/origin/",
                        "cache/")
    # update hierarchy
    if "hierarchy" in myMSG["properties"].keys():
        msg_hierarchy = myMSG["properties"]["hierarchy"]
        msg_hierarchy = msg_hierarchy.replace(
                                          "origin",
                                          "cache")
        myMSG["properties"]["hierarchy"] = msg_hierarchy

    LOG.debug(" - updated msg is:"
                             + json.dumps(myMSG, indent=4)) 
    LOG.debug(" - new topic value is: " + str(file_topic))
    # write to msg_modified_store
    LOG.info(" - write modified message to msg_modified_store (data_id is: " + str(data_id))
    msg_filename = filename_data_id + ".json"
    modified_msg_file = os.path.join(msg_modified_store, msg_filename)
    with open(modified_msg_file, 'w') as msgStoreMod_msg:
        msgStoreMod_msg.write(json.dumps(myMSG, indent=4))
    if monitor_metrics == "True":
        monToPub.labels(topic_country).inc()
        monToPub.labels("ALL").inc()
    # write ToPub file
    if "." in filename_data_id:
        filename_data_id = filename_data_id.replace(".","_")
    if ":" in filename_data_id:
        filename_data_id = filename_data_id.replace(":","_")
    toPubFilename_inWork = ".toPublish_" +  filename_data_id + ".json"
    toPubFile = os.path.join(toPubDir, toPubFilename_inWork)
    toPub_filecontent = '{"data_id":"' + str(data_id) + '", "fullURL":"' + str(fullURL) + '", "topic":"' + str(file_topic) + '"}'
    LOG.debug(" - toPub_filecontent is: " + str(toPub_filecontent))
    if not os.path.exists(toPubFile):
        with open(toPubFile, "w") as myToPubFile:
            myToPubFile.write(toPub_filecontent)
        toPubFilename_ready = toPubFilename_inWork.replace(".toPublish","toPublish")
        toPubFile_ready = os.path.join(toPubDir, toPubFilename_ready)
        shutil.move(toPubFile, toPubFile_ready)
    else:
        LOG.error(" toPubFile already there: " + str(toPubFile))


def checkBeforeDownload(download_formats, downloadFile):
    format_match = False
    file_extension = downloadFile.split(".")[1]
    for format_entry in download_formats:
        if format_entry in file_extension:
            LOG.debug(" - file format of: "
                      + str(downloadFile)
                      + " matches download format: "
                      + format_entry)
            format_match = True
    if format_match is False:
        LOG.warning(" - file format of: "
                    + str(downloadFile)
                    + " NOT matches any download format of config")
    return format_match


def download(url, downloadFile, integrity,
             integrity_method, data_id,
             todoJson, pub_client, topic_country):
    global errorDir, numThreads, monThreads, monDownloads, monOpendata, monErrors
    numThreads = numThreads + 1
    if monitor_metrics == "True":
        monThreads.inc()
    file_topic = topic
    myHeaders = ""
    topic_publisher = ""
    if "/" in data_id:
        filename_data_id = data_id.replace("/", dataId_replace)
    else:
        filename_data_id = data_id
    if download_restricted == "True":
        LOG.info(" - download from restricted source url: " + str(url))
        if download_username == "" or download_password == "":
            if download_token == "":
                LOG.error(" - missing username, password or token \
                          in config file for restricted download")
            else:
                myAuth = "Bearer " + download_token
                myHeaders = {"Authorization": myAuth}
                if download_proxy != "":
                    http_proxy = urllib3.ProxyManager(
                                   download_proxy,
                                   headers=myHeaders)
        else:
            myAuth = download_username + ":" + download_password
            myHeaders = urllib3.util.make_headers(basic_auth=myAuth)
            if download_proxy != "":
                http_proxy = urllib3.ProxyManager(
                                  download_proxy,
                                  headers=myHeaders)
    else:
        if download_proxy != "":
            http_proxy = urllib3.ProxyManager(download_proxy)
    http = urllib3.PoolManager()

    if downloadFile != "":
        if download_flat == "True":
            targetDir = ""
        else:
            targetDir = os.path.dirname(downloadFile)
    else:
        LOG.error(" - missing downloadFile value in json file")
        move_json2error(todoJson)
        LOG.info(" - moved to error toDownload JSON file: " + str(todoJson))
        targetDir = ""
    if targetDir != "" and not os.path.exists(targetDir):
        os.makedirs(targetDir)
    infoFile = os.path.join(local_memory, filename_data_id)
    LOG.debug("infoFile is: " + str(infoFile))

    if downloadFile != "":
        start_download = False
        if download_formats != []:
            start_download = checkBeforeDownload(download_formats, downloadFile)
        else:
            start_download = True

        tmp_filename = os.path.basename(downloadFile)
        tmp_targetDir = os.path.dirname(downloadFile)
        tmp_filename_dot = "." + tmp_filename
        tmp_file = tmp_targetDir + "/" + tmp_filename_dot
        if start_download:
            LOG.debug(" - start downloading file to tmp file: " + tmp_file)
            if not os.path.exists(tmp_targetDir):
                LOG.debug(" - create tmp_targetDir: " + tmp_targetDir)
                os.makedirs(tmp_targetDir)
            with open(tmp_file, 'wb') as out_tmp_file:
                retry = 1
                reqStatus = 999
                for retry in range(3):
                    try:
                        if myHeaders != "":
                            LOG.debug(" - myHeaders: " + str(myHeaders))
                            response = http.request(
                                  'GET',
                                  url,
                                  headers=myHeaders,
                                  preload_content=False)
                        else:
                            if download_proxy != "":
                                LOG.debug(" - proxy for http request is: " + str(download_proxy))
                                response = http_proxy.request(
                                             'GET',
                                             url,
                                             preload_content=False)
                            else:
                                response = http.request(
                                            'GET',
                                            url,
                                            preload_content=False)
                        reqStatus = response.status
                        if reqStatus != 200:
                            if retry == 2:
                                time.sleep(4)
                            retry = retry + 1
                        else:
                            retry = 4
                    except urllib3.exceptions.HTTPError as err:
                            LOG.error(" - http request error for: "
                                    + downloadFile)
                            LOG.error(err)
                            response = "http_error"
                if reqStatus != 200:
                    LOG.error(" - http response status: "
                              + str(reqStatus) + " for: " + str(url))
                    response = "http_error"
                if str(response) != "http_error":
                    shutil.copyfileobj(response, out_tmp_file)
                    LOG.debug(" - http request ready for: " + str(downloadFile))
                else:
                    out_tmp_file.truncate(0)
                    LOG.error(" - download_error occured for " + str(downloadFile) + ", URL was: " + str(url) + " and response status was: " +  str(reqStatus))
                    if monitor_metrics == "True":
                        monErrors.labels(topic_country).inc()
                        monErrors.labels("ALL").inc()
            if os.path.isfile(tmp_file):
                if os.path.getsize(tmp_file) > 0:
                    # rename only if not AFD 
                    if download_afdDir == "":
                        shutil.move(tmp_file, downloadFile)
                        LOG.debug(" - filesize > 0, copied tmp file to: "
                                  + str(downloadFile))
                        LOG.info(" - DOWNLOAD READY: " + str(downloadFile))
                        if monitor_metrics == "True":
                            monDownloads.labels(topic_country).inc()
                            monDownloads.labels("ALL").inc()
                    else:
                        # AFD
                        # get prefix for filename,
                        # also used for subdirecories on opendata
                        LOG.debug(" - topic is: " + str(file_topic))
                        if "wis2/" in file_topic:
                            topic_publisher = file_topic.split("wis2/")[1]
                            if "/data" in topic_publisher:
                                topic_publisher = topic_publisher.split("/data")[0]
                            else:
                                LOG.error("message not for data")
                        else:
                            LOG.error(" - missing wis2 in topic")
                        if not os.path.exists(download_afdDir):
                            os.makedirs(download_afdDir, exist_ok=True)
                        if "/" in topic_publisher:
                            topic_publisher_filename = topic_publisher.replace(
                                                            "/",
                                                            dataId_replace)
                        else:
                            topic_publisher_filename = topic_publisher

                        tmp_afdfile = os.path.join(
                                      download_afdDir,
                                      tmp_filename_dot)
                        tmp_filename = topic_publisher_filename \
                            + dataId_replace + tmp_filename
                        afd_out_file = os.path.join(download_afdDir, tmp_filename)

                        shutil.copyfile(tmp_file, tmp_afdfile)
                        shutil.move(tmp_afdfile, afd_out_file)
                        LOG.info(" - COPIED to AFD_DIR file: "
                                 + str(afd_out_file))
                        shutil.move(tmp_file, downloadFile)
                        LOG.info(" - DOWNLOAD READY: " + str(downloadFile))
                        if monitor_metrics == "True":
                            monDownloads.labels(topic_country).inc()
                            monDownloads.labels("ALL").inc()

                else:
                    LOG.error(" - download error: tmp filesize is 0")
                    move_json2error(todoJson)
                    LOG.info(" - moved to error toDownload JSON file: " + str(todoJson))
                    os.remove(tmp_file)
                    if monitor_metrics == "True":
                        monErrors.labels(topic_country).inc()
                        monErrors.labels("ALL").inc()
        else:
            # checkBeforeDownload result NO download, format error
            move_json2error(todoJson)
            if monitor_metrics == "True":
                monErrors.labels(topic_country).inc()
                monErrors.labels("ALL").inc()
    if os.path.exists(downloadFile):
        # write local memory file and remove toDo-json
        if not os.path.exists(infoFile):
            Path(infoFile).touch()
            LOG.info(" add file to local_memory: " +str(infoFile))
        else:
            LOG.debug(" - infoFile is: "
                      + str(infoFile))
        LOG.info(" calculate integrity for: " + str(downloadFile))
        integrity_file(downloadFile, infoFile, integrity_method)
        src_exists = os.path.exists(todoJson)
        if src_exists:
            os.remove(todoJson)
            LOG.info(" - removed toDownload (download ready): " + str(todoJson))
        else:
            LOG.info(" - todoJson (" + str(todoJson) + ") already removed")

        # add toPub and modify message for global cache
        if toPublish == "True":
            modifyAndAddToPub(data_id, downloadFile, file_topic, topic_country, topic_publisher)
            if monitor_metrics == "True":
                monOpendata.labels(topic_country).inc()
                monOpendata.labels("ALL").inc()
    # thread ready
    if numThreads > 0:
        numThreads = numThreads - 1
        if monitor_metrics == "True":
            monThreads.dec(1)


def calc_integrity(filename, integrity_method):
    if os.path.basename(filename) == "" or os.path.basename(filename) is None:
        LOG.error(" - calculating integrity missing filename: "
                  + os.path.basename(filename))
        file_integrity = "None"
    else:
        f = open(filename, 'rb')
        file_content = f.read()
        f.close()
        h = None
        if ("md5" in integrity_method):
            h = hashlib.md5()
        else:
            if ("sha512" in integrity_method or
               "SHA512" in integrity_method):
                h = hashlib.sha512()
            else:
                if ("sha256" in integrity_method or
                   "SHA256" in integrity_method):
                    h = hashlib.sha256()
                else:
                    LOG.error(" - integrity_method ('"
                              + integrity_method
                              + "') not known, filename: "
                              + filename)
        if h is not None:
            if file_content != "" and file_content is not None:
                h.update(file_content)
                file_integrity = str(base64.b64encode(h.digest()), "utf-8")
            else:
                LOG.error(" - calculating integrity: \
                          no file_content for file: " + filename)
                file_integrity = "None"
        else:
            file_integrity = "None"
    LOG.debug(" - calculated integrity for file "
              + str(filename)
              + " with value: " + str(file_integrity))
    return file_integrity


def integrity_file(filename, infoFile, integrity_method):
    file_integrity = calc_integrity(filename, integrity_method)
    try:
        with open(infoFile, 'w') as attrFile:
            attrFile.write(file_integrity)
            LOG.debug(" - integrity value written to info file: " + str(infoFile))
    except Exception as err:
        LOG.error(" - while writing integrtiy to infoFile: " + str(infoFile) + ", error was: " + str(err))


def createNewDownloadThread(url, downloadFileLocation,
                            integrity, integrity_method,
                            data_id, todoJson, pub_client, topic_country):
    global infoFile
    if (url == "" or downloadFileLocation == "" or data_id == ""):
        LOG.error(" - createNewDownloadThread: missing url, \
                  downloadFileLocation or data_id value")
        LOG.error(" - url value is: " + str(url)
                  + " / downloadFileLocation is: "
                  + str(downloadFileLocation)
                  + " / data_id is: " + str(data_id))
        move_json2error(todoJson)
    else:
        download_thread = threading.Thread(
                                 target=download,
                                 args=(
                                    url,
                                    downloadFileLocation,
                                    integrity,
                                    integrity_method,
                                    data_id,
                                    todoJson,
                                    pub_client,
                                    topic_country))
        download_thread.start()


def connect_prometheus(prometheus_port):
    try:
        start_http_server(prometheus_port)
    except:
        print("Can not listen on " + str(prometheus_port))


def init_metric():
    global monToDoJson, monThreads, monErrors, monDownloads, monToPub, monOpendata, monNotCore, monDuplicate, monWithContent
    connect_prometheus(PROMETHEUS_PORT)
    REGISTRY.unregister(PROCESS_COLLECTOR)
    REGISTRY.unregister(PLATFORM_COLLECTOR)
    REGISTRY.unregister(REGISTRY._names_to_collectors['python_gc_objects_collected_total'])

    monToDoJson = Gauge('msg_download_toDoJson', 'Amount of toDo json files for countryy', ["Country"])
    monToDoJson.labels("ALL").set(0)
    monErrors = Gauge('msg_download_errors', 'Amount of errors for download for Country', ["Country"])
    monErrors.labels("ALL").set(0)
    monNotCore = Gauge('msg_not_core', 'Amount of messages not core for countryy', ["Country"])
    monNotCore.labels("ALL").set(0)
    monToPub = Gauge('msg_download_toPub4GC', 'Amount of messages to publish for GC for country', ["Country"])
    monToPub.labels("ALL").set(0)
    monDownloads = Gauge('msg_downloaded', 'Amount of downloads for country', ["Country"])
    monDownloads.labels("ALL").set(0)
    monOpendata = Gauge('msg_download_toOpendata', 'Amount of moved files to opendata for country', ["Country"])
    monOpendata.labels("ALL").set(0)
    monDuplicate = Gauge('msg_download_duplicate', 'Amount of messages already downloaded for country', ['Country'])
    monDuplicate.labels("ALL").set(0)
    monWithContent = Gauge('msg_download_content', 'Amount of messages with content for country', ['Country'])
    monWithContent.labels("ALL").set(0)
    monThreads =  Gauge('msg_download_threads', 'Amount of Threads for downloading')
    monThreads.set(0)


# declaration
msg_counter_days = {}
metric_countries = ["ALL"]
integrity_method = ""
withDownload = "False"
download_targetDir = ""
download_toDoDir = ""
filedownloadURL = ""
loggerName = "download" + str(configFile)
LOG = None
# if data_id includes '/' it should be replaced by...
dataId_replace = "___"


# read config file values
if config_filename == "":
    print("error - no config file.")
else:
    # read config
    with open(config_filename, 'r') as myConfigFile:
        data = myConfigFile.read()
    myConfig = json.loads(data)

    if "withDownload" in myConfig.keys():
        withDownload = myConfig["withDownload"]
    else:
        print("error -  config file - missing value: \
              withDownload, set to False")
        withDownload = "False"
    if withDownload == "True":
        if "download_targetDir" in myConfig.keys():
            download_targetDir = myConfig["download_targetDir"]
        else:
            print("error -  config file - missing value: \
                  download_targetDir, set to local targetDownload")
            download_targetDir = "targetDownload/"
        if "download_toDoDir" in myConfig.keys():
            download_toDoDir = myConfig["download_toDoDir"]
        else:
            print("error -  config file - missing value: \
                  download_toDoDir, set to local toDownload")
            download_toDoDir = "toDownload/"
        if inputDir == "":
            inputDir = download_toDoDir
        if "filedownloadURL" in myConfig.keys():
            filedownloadURL = myConfig["filedownloadURL"]
        else:
            print("error -  config file - missing value: \
                  filedownloadURL, set to True")
            filedownloadURL = "True"
        if "download_proxy" in myConfig.keys():
            download_proxy = myConfig["download_proxy"]
        else:
            print("error -  config file - missing value: \
                  download_proxy, set to ''")
            download_proxy = ""
        if "download_restricted" in myConfig.keys():
            download_restricted = myConfig["download_restricted"]
        else:
            print("error -  config file - missing value: \
                  download_restricted, set to False")
            download_restricted = "False"
        if download_restricted == "True":
            if "download_username" in myConfig.keys():
                download_username = myConfig["download_username"]
            else:
                print("error -  config file - missing value: \
                      download_username, set to ''")
                download_username = ""
            if "download_password" in myConfig.keys():
                download_password = myConfig["download_password"]
            else:
                print("error -  config file - missing value: \
                      download_password, set to ''")
                download_password = ""
            if "download_token" in myConfig.keys():
                download_token = myConfig["download_token"]
            else:
                print("error -  config file - missing value: \
                      download_token, set to ''")
                download_token = ""
        if "download_logfile" in myConfig.keys():
            download_logfile = myConfig["download_logfile"]
        else:
            print("error -  config file - missing value: \
                  download_logfile, set to download.log")
            download_logfile = "download.log"
        if "download_loglevel" in myConfig.keys():
            download_loglevel = myConfig["download_loglevel"]
        else:
            print("error -  config file - missing value: \
                  download_loglevel, set to INFO")
            download_loglevel = "INFO"
        if "download_flat" in myConfig.keys():
            download_flat = myConfig["download_flat"]
        else:
            print("error -  config file - missing value: \
                  download_flat, set to False \
                  - topic value is used as subtree")
            download_flat = "False"
        if "download_afdDir" in myConfig.keys():
            download_afdDir = myConfig["download_afdDir"]
        else:
            print("error -  config file - missing value: \
                  download_afdDir, set to '' - use no afdDir")
            download_afdDir = ""
        if "download_formats" in myConfig.keys():
            download_formats = myConfig["download_formats"]
        else:
            print("error -  config file - missing value: \
                  download_formats set to [] - download all formats")
            download_formats = []
        if "download_onlyCore" in myConfig.keys():
            download_onlyCore = myConfig["download_onlyCore"]
        else:
            print("error -  config file - missing value: \
                  download_onlyCore, set to True")
            download_onlyCore = "True"

        # publish
        if "toPublish" in myConfig.keys():
            toPublish = myConfig["toPublish"]
        else:
            print("error -  config file - missing value: \
                  toPublish, set to False")
            toPublish = "False"
        if toPublish == "True":
            if "msg_store" in myConfig.keys():
                msg_store = myConfig["msg_store"]
            else:
                print("error - config file - missing value: \
                      msg_store, set to ''")
                msg_store = ""
            if msg_store == "":
                print("error -  config file - missing value: \
                      msg_store, is needed if toPublish is True")
            if "pub_URLbase" in myConfig.keys():
                pub_URLbase = myConfig["pub_URLbase"]
            else:
                print("error - config file - missing value: \
                      pub_URLbase, set to ''")
                pub_URLbase = ""
            if "sub_maxMSGsize" in myConfig.keys():
                sub_maxMSGsize = myConfig["sub_maxMSGsize"]
            else:
                print("error - config file - missing value: \
                      sub_maxMSGsize, set to 2048")
                sub_maxMSGsize = 2048

        # write monitor metrics
        if "monitor_metrics" in myConfig.keys():
            monitor_metrics = myConfig["monitor_metrics"]
        else:
            print("error -  config file - missing value: \
                   monitor_metrics, set to False")
            monitor_metrics = "False"
        # monitoring
        if monitor_metrics == "True":
            if "PROMETHEUS_PORT" in myConfig.keys():
                PROMETHEUS_PORT = myConfig["PROMETHEUS_PORT"]
                PROMETHEUS_PORT = int(PROMETHEUS_PORT) + 1
            else:
                print("error -  config file - missing value: \
                      PROMETHEUS_PORT, set to 12001")
                PROMETHEUS_PORT=12001
            init_metric()
        else:
            monToDoJson = None
            monThreads = None
            monErrors = None
            monDownloads = None
            monToPub = None
            monOpendata = None
            monNotCore = None
            monDuplicate = None


# programm
init_log(download_logfile, download_loglevel, loggerName)
LOG.info(" ----- STARTED download.py -----")
if download_flat == "True":
    LOG.info(" - flat download")
if download_toDoDir.endswith("/"):
    local_memory = download_toDoDir + "done/"
    toPubDir = download_toDoDir + "toPub/"
else:
    local_memory = download_toDoDir + "/done/"
    toPubDir = download_toDoDir + "/toPub/"
if toPublish == "True":
    if msg_store.endswith("/"):
        msg_modified_store = msg_store + "modified/"
    else:
        msg_modified_store = msg_store + "/modified/"
    if not os.path.exists(local_memory):
        os.makedirs(local_memory, exist_ok=True)
    if not os.path.exists(toPubDir):
        os.makedirs(toPubDir, exist_ok=True)
    if not os.path.exists(msg_modified_store):
        os.makedirs(msg_modified_store, exist_ok=True)

pub_client = None
pub_connected_flag = False

if os.path.isdir(inputDir):
    numThreads = 0
    while True:
        readInput_timeNow = datetime.now()
        readInput_printDay = readInput_timeNow.strftime('%Y%m%d')
        readInput_printNow = readInput_timeNow.strftime('%Y%m%d%H%M%S%f')
        # add new day as item to msg_counter_days and reset metric counters
        init_counter(readInput_printDay)
        # list of toDoFiles from input dir
        inputFiles = [f for f in os.listdir(
                                   inputDir) if os.path.isfile(
                                                  os.path.join(inputDir, f)) and not f.startswith('.')]
        LOG.debug(" - inputFiles len is: " + str(len(inputFiles)))
        if len(inputFiles) == 0:
            LOG.info(" - WAITING for input files")
            time.sleep(2)
        else:
            LOG.info(" - input files count is: " + str(len(inputFiles)))
            # update value for monToDoJson
            if monitor_metrics == "True":
                msg_counter_days[readInput_printDay] = msg_counter_days[readInput_printDay] + len(inputFiles)
                monToDoJson.labels("ALL").set(msg_counter_days[readInput_printDay])
            LOG.info("New Downloads arrived: " + str(len(inputFiles)) + " files, number of active threads: " + str(numThreads))
        while len(inputFiles) > 0:
            LOG.debug(" number of active threads: " + str(numThreads))
            for item in inputFiles:
                if numThreads < 100:
                    try:
                        if ".nfs" not in item:
                            src_todo = os.path.join(inputDir, item)
                            Json_inWork_name = "." + item
                            Json_inWork = os.path.join(inputDir, Json_inWork_name)
                            shutil.move(src_todo, Json_inWork)
                            LOG.info(" - next toDownload is: " + str(src_todo))
                            timeNow = datetime.now()
                            printTimeNow = timeNow.strftime('%Y%m%dT%H%M%S')
                            # set to default
                            downloadFile = ""
                            integrity = ""
                            sourceUrl = ""
                            integrity_method = ""
                            sameContent = False
                            sumstr = ""
                            targetDir = ""
                            msg_content = ""
                            filecontent = ""
                            data_id = ""
                            LOG.debug(" - next download is: " + item)
                            topic = ""
                            topic_country = ""
                            # read toDo json file
                            myFile = open(Json_inWork, "r")
                            filecontent = myFile.read()
                            myFile.close()
                            errorDir = inputDir + "/errors/"
                            try:
                                json_file = json.loads(filecontent)
                                if "content_value" in json_file.keys():
                                    LOG.debug(" - content included in message")
                                    topic = json_file["topic"]
                                    targetDir = download_targetDir
                                    msg_content = json_file["content_value"]
                                else:
                                    if "downloadFilename" in json_file.keys():
                                        downloadFile = json_file["downloadFilename"]
                                    else:
                                        LOG.info(" - missing value in JSON: \
                                         downloadFilename, set to ''")
                                    if "integrity" in json_file.keys():
                                        integrity = json_file["integrity"]
                                    else:
                                        LOG.info(" - missing value in JSON: \
                                         integrity, set to ''")
                                    if "sourceUrl" in json_file.keys():
                                        sourceUrl = json_file["sourceUrl"]
                                    else:
                                        LOG.info(" - missing value in JSON: \
                                         sourceUrl, set to ''")
                                    if "integrity_method" in json_file.keys():
                                        integrity_method = json_file["integrity_method"]
                                    else:
                                        LOG.info(" - missing value in JSON: \
                                         integrity_method, set to ''")
                                    if integrity_method == "":
                                        LOG.error(" missing integrity_method in Json toDownload " + str(Json_inWork) + ", set to sha512")
                                        integrity_method = "sha512"
                                    if "topic" in json_file.keys():
                                        topic = json_file["topic"]
                                        # monitoring metrics
                                        if "/" in topic:
                                            topic_country = topic.split("/")[4]
                                            LOG.debug(" - topic_country is: " + str(topic_country))
                                        else:
                                            LOG.error(" - MISSING '/' in topic, in json file: "
                                                      + str(json.dumps(msg_content, indent=4)))
                                        if topic_country not in metric_countries:
                                            metric_countries.append(topic_country)
                                            init_metric_label(topic_country)
                                        if monitor_metrics == "True":
                                            monToDoJson.labels(topic_country).inc()
                                            monToDoJson.labels("ALL").inc()
                                    else:
                                        LOG.info(" - missing value in JSON: \
                                         topic, set to ''")
                                    
                                    if "data_id" in json_file.keys():
                                        data_id = json_file["data_id"]
                                    else:
                                        LOG.error(" - missing value in JSON: \
                                             data_id, set to ''")
                                    if "/" in data_id:
                                        dataId_dir = os.path.dirname(data_id)
                                        filename_data_id = data_id.replace(
                                                    "/",
                                                    dataId_replace)
                                    else:
                                        dataId_dir = ""
                                        filename_data_id = data_id
                                    infoFile = local_memory + filename_data_id

                                    if (downloadFile == "" or sourceUrl == ""):
                                        LOG.error(" - jsonFile toDownload: \
                                          missing downloadFile \
                                          or sourceUrl, json is: "
                                          + str(filecontent))
                                        if monitor_metrics == "True":
                                            monErrors.labels("ALL").inc()
                                            monErrors.labels(topic_country).inc()
                                    if download_flat == "True":
                                        targetDir = ""
                                    else:
                                        targetDir = os.path.dirname(downloadFile)
                            except Exception as e:
                                LOG.error(" - while reading json toDo, error was: " + str(e))
                                errorFile = os.path.join(errorDir, item)
                                shutil.move(Json_inWork, errorFile)
                                if monitor_metrics == "True":
                                    monErrors.labels("ALL").inc()
                                    monErrors.labels(topic_country).inc()
                            if targetDir != "":
                                errorDir = download_targetDir + "/errors/"
                                if not os.path.exists(targetDir):
                                    LOG.info(" - create targetDir: " + targetDir)
                                    os.makedirs(targetDir)
                            errorDir = errorDir.replace("//", "/")
                            if not os.path.exists(errorDir):
                                LOG.info(" - create errorDir: " + errorDir)
                                os.makedirs(errorDir)
                            if "content_value" in json_file.keys():
                                # msg with data in content field
                                msgContentFile = targetDir + "/" \
                                         + topic.replace(".", "/") \
                                         + "/" + "msgWithContent_" \
                                         + printTimeNow + ".txt"
                                with open(msgContentFile, "w") as content2file:
                                    if "content_value" in json_file.keys():
                                        content2file.write(msg_content)
                                LOG.warning(" - MSG WITH CONTENT: " + str(msgContentFile))
                                if monitor_metrics == "True":
                                    monWithContent.labels("ALL").inc()
                                    monWithContent.labels(topic_country).inc()
                                os.remove(Json_inWork)
                                LOG.info(" - removed toDownload: " + str(Json_inWork))
                            else:
                                # file already exists?
                                if download_flat == "True":
                                    downloadFilename = os.path.basename(
                                                  downloadFile)
                                    downloadFile = os.path.join(
                                               download_targetDir,
                                               downloadFilename)
                                if os.path.exists(downloadFile):
                                    LOG.info(" - File already downloaded")
                                    if integrity != "":
                                        # retrieve old checksum
                                        if os.path.exists(infoFile):
                                            sameIntegrity(infoFile, integrity)
                                            if sameContent is True:
                                                LOG.info(" - SAME content,\
                                                 no download for: "
                                                 + downloadFile)
                                                if monitor_metrics == "True":
                                                    monDuplicate.labels("ALL").inc()
                                                    monDuplicate.labels(topic_country).inc()
                                            else:
                                                LOG.error("NOT SAME integrity value, \
                                                  calc_integrity for: "
                                                  + downloadFile)
                                                integrity_new = calc_integrity(
                                                          downloadFile,
                                                          integrity_method)
                                                if integrity_new == integrity:
                                                    LOG.info("SAME integrity value, \
                                                     NO download")
                                                    if monitor_metrics == "True":
                                                        monDuplicate.labels("ALL").inc()
                                                        monDuplicate.labels(topic_country).inc()
                                                else:
                                                    LOG.info(" - integrity value NOT equal, \
                                                     download again...")
                                                    try:
                                                        createNewDownloadThread(
                                                             sourceUrl,
                                                             downloadFile,
                                                             integrity,
                                                             integrity_method,
                                                             data_id,
                                                             Json_inWork,
                                                             pub_client,
                                                             topic_country)
                                                    except BaseException as err:
                                                        LOG.error("NOT reached: "
                                                          + sourceUrl)
                                                        LOG.error(err)
                                                        move_json2error(Json_inWork)
                                                        LOG.info(" - moved to error: " + str(Json_inWork))
                                                        if monitor_metrics == "True":
                                                            monErrors.labels("ALL").inc()
                                                            monErrors.labels(topic_country).inc()
                                        else:
                                            LOG.error("MISSING: data_id in memory, \
                                              already downloaded")
                                            Path(infoFile).touch()
                                            # calculate and write integrity
                                            integrity_file(downloadFile,
                                                   infoFile,
                                                   integrity_method)
                                    else:
                                        alreadyThere = os.path.exists(infoFile)
                                        if alreadyThere is True:
                                            os.remove(Json_inWork)
                                            LOG.info(" - removed toDownload (already there): " + str(Json_inWork))
                                            LOG.info("MISSING: integrity value in msg, \
                                             already downloaded: "
                                             + downloadFile)
                                            if monitor_metrics == "True":
                                                monDuplicate.labels("ALL").inc()
                                                monDuplicate.labels(topic_country).inc()
                                else:
                                    if download_onlyCore:
                                        if "/recommended/" not in topic:
                                            try:
                                                createNewDownloadThread(
                                                          sourceUrl,
                                                          downloadFile,
                                                          integrity,
                                                          integrity_method,
                                                          data_id,
                                                          Json_inWork,
                                                          pub_client,
                                                          topic_country)
                                            except BaseException as err:
                                                LOG.error(" - createNewDownloadThread: "
                                                        + sourceUrl + " error was: " + str(err))
                                                move_json2error(Json_inWork)
                                                if monitor_metrics == "True":
                                                    monErrors.labels("ALL").inc()
                                                    monErrors.labels(topic_country).inc()
                                        else:
                                            LOG.info(" - download only core: "
                                             + str(topic))
                                            os.remove(Json_inWork)
                                            LOG.info(" - removed toDownload (not core): " + str(Json_inWork))
                                            if monitor_metrics == "True":
                                                monNotCore.labels(topic_country).inc()
                                                monNotCore.labels("ALL").inc()
                                    else:
                                        try:
                                            createNewDownloadThread(
                                                    sourceUrl,
                                                    downloadFile,
                                                    integrity,
                                                    integrity_method,
                                                    data_id,
                                                    Json_inWork,
                                                    pub_client,
                                                    topic_country)
                                            if "/recommended/" in topic:
                                                if monitor_metrics == "True":
                                                    monNotCore.labels("ALL").inc()
                                                    monNotCore.labels(topic_country).inc()
                                        except BaseException as e:
                                            LOG.error(" - createNewDownloadThread: "
                                              + sourceUrl + " error was: " + str(e))
                                            move_json2error(Json_inWork)
                                            if monitor_metrics == "True":
                                                monErrors.labels("ALL").inc()
                                                monErrors.labels(topic_country).inc()
                    except Exception as e:
                        LOG.error(" - global try except error occurred")
                        LOG.error(e)
                        if monitor_metrics == "True":
                            monErrors.labels("ALL").inc()
                    inputFiles.remove(item)
                    LOG.debug(" - inputFiles reduced to: " + str(len(inputFiles)))
                else:
                    LOG.warning(" - all threads in use")
                    time.sleep(2)
else:
    LOG.error(" - inputDir (toDoDir) not found")
