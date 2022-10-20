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


def init_counter(date):
    global msg_counter
    counter_timeNow = datetime.now()
    counter_printDay = counter_timeNow.strftime('%Y%m%d')
    if counter_printDay not in msg_counter.keys():
        msg_counter.update({counter_printDay: 0})



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
        shutil.copy(todoJson, errorFile)
        os.remove(todoJson)
    else:
        LOG.error(" - missing toDo JSON: " + str(todoJson))
    LOG.error(" - download error occured for json ( "
              + str(todoJson) + " ), move json to "
              + str(errorFile))


def modifyMSGAndPub(myMSG, file_topic, fullURL):
    global pub_connected_flag, monitor_metrics, monPub, monErrors
    # modify org_msg links to opendata links
    new_msg_id = str(uuid.uuid1())
    myMSG["id"] = new_msg_id
    cache_pub_now = time.time()
    cache_pub_nsec = ('%.9g' % (cache_pub_now % 1))[1:]
    cache_pub_datestamp = time.strftime(
                         "%Y%m%dT%H%M%S",
                         time.gmtime(cache_pub_now)) + cache_pub_nsec
    gcTime = cache_pub_datestamp + "Z"
    if "pubtime" in myMSG["properties"].keys():
        myMSG["properties"]["pubtime"] = gcTime
    else:
        if "pub_datetime" in myMSG["properties"].keys():
            myMSG["properties"]["pub_datetime"] = gcTime
            LOG.warning(" - pub_datetime in original msg,\
             should be pubtime")
        if ("publication_datetime" in myMSG["properties"].keys()):
            myMSG["properties"]["publication_datetime"] = gcTime
            LOG.warning(" - publication_datetime in original msg,\
             should be pubtime")
        if "pubTime" in myMSG["properties"].keys():
            myMSG["properties"]["pubTime"] = gcTime
            LOG.warning(" - pubTime in original msg,\
             should be pubtime")

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
            LOG.info(" - no canonical href in links,\
                use first href value instead")
            msgStore_url = urlList[0]["href"]
        if msgStore_url == "":
            LOG.error(" - no links in message: "
                  + str(json.dumps(myMSG, indent=4)))
    else:
        if monitor_metrics == "True":
            monErrors.inc()
    # only publish own link for cached messages
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
    LOG.info(" - message modified for re-pub")
    # pub
    if pub_connected_flag is False:
        LOG.error(" - pub_client not connected, \
         try to re-connect")
        getPubConnection()
    if pub_connected_flag is False:
        LOG.error(" - pub_client not connected, \
                          NO Publish possible")
        if monitor_metrics == "True":
            monErrors.inc()
    else:
        info = pub_client.publish(
                           file_topic,
                           json.dumps(myMSG, indent=4),
                           qos=1,
                           properties=None)
        if info[0] != 0:
            LOG.error(" - PUBLISH FAILED, return code was: " + str(info))
            if monitor_metrics == "True":
                monErrors.inc()
        if monitor_metrics == "True":
            if "swe/smhi" in file_topic:
                monPub.labels("SWEDEN").inc()


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


def download(url, downloadFile, new_sum,
             integrity_method, data_id,
             todoJson, pub_client):
    global errorDir
    global numThreads
    global monThreads
    numThreads = numThreads + 1
    if monitor_metrics == "True":
        monThreads.inc()
    file_topic = topic
    myHeaders = ""
    topic_publisher = ""
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
        targetDir = ""
    if targetDir != "" and not os.path.exists(targetDir):
        os.makedirs(targetDir)

    if "/" in data_id:
        filename_data_id = data_id.replace("/", dataId_replace)
    else:
        filename_data_id = data_id
    infoFile = os.path.join(local_memory, filename_data_id)
    LOG.debug("infoFile is: " + str(infoFile))

    if downloadFile != "":
        start_download = False
        if download_formats != []:
            start_download = checkBeforeDownload(download_formats, downloadFile)
        else:
            LOG.info(" - no download format in config file, \
                                 download all")
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
                if myHeaders != "":
                    try:
                        LOG.debug(" - myHeaders: " + str(myHeaders))
                        response = http.request(
                                  'GET',
                                  url,
                                  headers=myHeaders,
                                  preload_content=False)
                        reqStatus = response.status
                        if reqStatus != 200:
                            LOG.error(" - http response status (with headers): "
                                      + str(reqStatus) + " for: " + str(url))
                            response = "http_error"
                    except urllib3.exceptions.HTTPError as err:
                        LOG.error(" - http request error for: "
                                  + downloadFile)
                        LOG.error(err)
                        response = "http_error"
                else:
                    # special case download own data (e.g. DWD for internal URL)
                    if "dwd.de" in url:
                        try:
                            response = http.request(
                                       'GET',
                                       url,
                                       preload_content=False)
                            reqStatus = response.status
                            if reqStatus != 200:
                                LOG.error(" - http response status (dwd-url) was: "
                                          + str(reqStatus) + " for: " + str(url))
                                response = "http_error"
                        except urllib3.exceptions.HTTPError:
                            LOG.error(" - http request error for: " + downloadFile)
                            response = "http_error"
                    else:
                        if download_proxy != "":
                            try:
                                response = http_proxy.request(
                                             'GET',
                                             url,
                                             preload_content=False)
                                reqStatus = response.status
                                if reqStatus != 200:
                                    LOG.error(" - http response status \
                                              (no headers, with proxy): "
                                              + str(reqStatus)
                                              + " for: " + str(url))
                                    response = "http_error"
                            except urllib3.exceptions.HTTPError:
                                LOG.error(" - http request error for: "
                                          + downloadFile
                                          + " from URL: " + str(url))
                                response = "http_error"
                        else:
                            try:
                                response = http.request(
                                            'GET',
                                            url,
                                            preload_content=False)
                                reqStatus = response.status
                                if reqStatus != 200:
                                    LOG.error(" - http response status \
                                          (no headers, no proxy): "
                                          + str(reqStatus)
                                          + " for: " + str(url))
                                    response = "http_error"
                            except urllib3.exceptions.HTTPError:
                                LOG.error(" - http request error for: "
                                          + downloadFile
                                          + " from URL: " + str(url))
                                response = "http_error"
                if str(response) != "http_error":
                    shutil.copyfileobj(response, out_tmp_file)
                    LOG.debug(" - http request ready for: " + str(downloadFile))
                else:
                    move_json2error(todoJson)
                    LOG.error(" - download_error occured, URL was: " + str(url))
                    if monitor_metrics == "True":
                        monErrors.inc()
            if os.path.isfile(tmp_file):
                if os.path.getsize(tmp_file) > 0:
                    # rename only if not AFD 
                    if download_afdDir == "":
                        shutil.move(tmp_file, downloadFile)
                        LOG.debug(" - filesize > 0, copied tmp file to: "
                                  + str(downloadFile))
                        LOG.info(" - DOWNLOAD READY: " + str(downloadFile))
                    # AFD
                    if download_afdDir != "":
                        # get prefix for filename,
                        # also used for subdirecories on opendata
                        LOG.info(" - topic is: " + str(file_topic))
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
                else:
                    LOG.error(" - download error: tmp filesize is 0")
                    move_json2error(todoJson)
                    os.remove(tmp_file)
                    if monitor_metrics == "True":
                        monErrors.inc()
        else:
            if monitor_metrics == "True":
                monErrors.inc()
    if os.path.exists(downloadFile):
        # write local memory file and remove toDo-json
        if not os.path.exists(infoFile):
            touch_memFile(local_memory, data_id)
        else:
            LOG.debug(" - infoFile is (before integrity_file method): "
                      + str(infoFile))
        if integrity_method != "":
            integrity_file(downloadFile, infoFile, integrity_method)
        LOG.debug(" - download without errors, remove json")
        src_exists = os.path.exists(todoJson)
        if src_exists:
            os.remove(todoJson)
        else:
            LOG.info(" - todoJson (" + str(todoJson) + ") already removed")

        # pub message for global cache
        if toPublish == "True":
            # read org msg from msg_store
            if data_id != "":
                if msg_store != "":
                    msgStore_path = os.path.dirname(msg_store)
                    if os.path.exists(msgStore_path):
                        msgStore_filename = data_id.replace(
                                               "/",
                                               dataId_replace)
                        msgStore_file = os.path.join(
                                             msgStore_path,
                                             msgStore_filename)
                        if os.path.exists(msgStore_file):
                            with open(msgStore_file, 'r') as msgStore_msg:
                                myMSG = msgStore_msg.read()
                            msgStore_msg = json.loads(myMSG)
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
            # check if downloaded file is arrived on opendata
            if not pub_URLbase.endswith("/"):
                fullURL = pub_URLbase + "/"
            else:
                fullURL = pub_URLbase
            fullURL = fullURL + topic_publisher
            downloaded_filename = os.path.basename(downloadFile)
            fullURL = fullURL + "/" + downloaded_filename
            LOG.info(" - fullURL whether transmitted: "
                     + str(fullURL))
            # test
            # file_topic = "cache_test/" + file_topic
            # request to check if file on opendata
            opendata_respStatus = 999
            # let afd work
            time.sleep(2)
            opendata_request = http.request(
                                       'GET',
                                       fullURL,
                                       preload_content=False)
            opendata_respStatus = opendata_request.status
            if opendata_respStatus != 200:
                # not on opendata, wait and try again 
                i = 1
                for i in range(2):
                    time.sleep(2)
                    opendata_request = http.request(
                                           'GET',
                                           fullURL,
                                           preload_content=False)
                    opendata_respStatus = opendata_request.status
                    if opendata_respStatus != 200:
                        i = i + 1
                    else:
                        i = 5
            if opendata_respStatus == 200:
                LOG.info(" - TRANSMITTED to opendata: " + str(fullURL))
                # pub message for cache
                if msgStore_msg is not None:
                    modifyMSGAndPub(msgStore_msg, file_topic, fullURL)
                else:
                    LOG.error(" - PUBLISH for cache message failed, \
                              missing msg_store message")
            else:
                LOG.error(" - file NOT transmitted to opendata, NO PUBLISH\
                        (response code: " + str(opendata_respStatus) + ")")
    # thread ready
    LOG.info(" - download end")
    if numThreads > 0:
        numThreads = numThreads - 1
        if monitor_metrics == "True":
            monThreads.dec(1)


def touch_memFile(local_memory, data_id):
    # touch memory_file with data_id as filename
    if "/" in data_id:
        filename_data_id = data_id.replace("/", dataId_replace)
    else:
        filename_data_id = data_id
    memory_file = os.path.join(local_memory, filename_data_id)
    memory_targetDir = os.path.join(local_memory)
    if not os.path.exists(memory_targetDir):
        LOG.info(" - created local memory directory: " + str(local_memory))
        os.makedirs(memory_targetDir, exist_ok=True)
    Path(memory_file).touch()


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
    with open(infoFile, 'w') as attrFile:
        attrFile.write(file_integrity)
        LOG.debug(" - integrity value written to info file: " + str(infoFile))


def createNewDownloadThread(url, downloadFileLocation,
                            integrity, integrity_method,
                            data_id, todoJson, pub_client):
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
                                    pub_client))
        download_thread.start()


# re-publish
def on_pubconnect(myPub_client, userdata, flags, rc, properties=None):
    global pub_connected_flag
    LOG.info(" - pub_connection code is: " + str(rc))
    if rc == 0:
        LOG.debug(" - connected to mqtts broker for publish for global cache")
        pub_connected_flag = True
    else:
        LOG.error(" - connection to mqtt broker for global cache pub failed \
                  with result code: " + str(rc))


def on_pubdisconnect(pub_client, userdata, rc, properties=None):
    global pub_connected_flag
    LOG.info(" - pub_client disconnected")
    pub_connected_flag = False


def getPubConnection():
    global pub_client
    global pub_connected_flag
    client_suffix = uuid.uuid1()
    clientname = socket.gethostname() + "_4pub_" + str(client_suffix)
    LOG.info("pub_clientname is: " + str(clientname))
    if pub_protocol_version == "MQTTv5":
        pub_client = mqtt.Client(
                         clientname,
                         protocol=mqtt.MQTTv5)
    else:
        pub_client = mqtt.Client(
                         clientname,
                         clean_session=True,
                         protocol=mqtt.MQTTv311)
    pub_connected_flag = False
    pub_client.username_pw_set(
                        pub_user,
                        password=pub_password)
    pub_client.tls_set(
                 tls_version=ssl.PROTOCOL_TLSv1_2)
    pub_client.on_connect = on_pubconnect
    pub_client.on_disconnect = on_pubdisconnect
    pub_client.on_publish = on_publish
    if pub_protocol_version == "MQTTv5":
        pubConn_properties = Properties(PacketTypes.CONNECT)
        pubConn_properties.MaximumPacketSize = sub_maxMSGsize
    else:
        pubConn_properties = None
    LOG.debug("CONNECT properties for pub are: "
              + str(pubConn_properties))
    pub_client.connect(
                  pub_host,
                  port=int(pub_port),
                  properties=pubConn_properties)
    pub_client.loop_start()


def on_publish(client, userdata, mid):
    global monPub
    LOG.info(" - on_publish, message id {}".format(mid))
    LOG.info(" - published successfully to mqtt broker")
    if monitor_metrics == "True":
        monPub.labels("ALL").inc()


def connect_prometheus(prometheus_port):
    try:
        start_http_server(prometheus_port)
    except:
        print("Can not listen on " + str(prometheus_port))


def init_metric():
    global monToDoJson, monPub, monThreads, monErrors
    connect_prometheus(PROMETHEUS_PORT)
    REGISTRY.unregister(PROCESS_COLLECTOR)
    REGISTRY.unregister(PLATFORM_COLLECTOR)
    REGISTRY.unregister(REGISTRY._names_to_collectors['python_gc_objects_collected_total'])

    monToDoJson = Gauge('msg_download_toDoJson', 'Amount of toDo json files')
    monToDoJson.set(0)

    monErrors = Gauge('msg_download_errors', 'Amount of errors for download for toDo json files')
    monErrors.set(0)

    monPub = Gauge('msg_pub4GC', 'Amount of messages published for GC for country', ["Country"])
    monPub.labels("SWEDEN").set(0)
    monPub.labels("ALL").set(0)

    monThreads =  Gauge('msg_download_threads', 'Amount of Threads for downloading')
    monThreads.set(0)


# declaration
msg_counter = {}
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
            if "pub_host" in myConfig.keys() and \
               "pub_port" in myConfig.keys() and \
               "pub_user" in myConfig.keys() and \
               "pub_password" in myConfig.keys():
                pub_host = myConfig["pub_host"]
                pub_port = myConfig["pub_port"]
                pub_user = myConfig["pub_user"]
                pub_password = myConfig["pub_password"]
            else:
                print("error - config file - missing value: \
                      pub_host, pub_port, pub_user and/or pub_password")
            pub_protocol = "mqtts"
            if "pub_protocol" in myConfig.keys():
                pub_protocol = myConfig["pub_protocol"]
            if pub_protocol != "mqtts":
                print("error - config file: pub_protocol not mqtts")
            else:
                pub_message_broker = "mqtts://" + pub_user \
                                     + ":[passwd]@" + pub_host \
                                     + ":" + str(pub_port)
                if "pub_protocol_version" in myConfig.keys():
                    pub_protocol_version = myConfig["pub_protocol_version"]
                else:
                    print("error - config file - missing value: \
                          pub_protocol_version, set to MQTTv5")
                    pub_protocol_version = "MQTTv5"
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
            if "PROMETHEUS_DOWNLOAD_PORT" in myConfig.keys():
                PROMETHEUS_PORT = myConfig["PROMETHEUS_DOWNLOAD_PORT"]
            else:
                print("error -  config file - missing value: \
                      PROMETHEUS_DOWNLOAD_PORT, set to 12001")
                PROMETHEUS_PORT=12001
            init_metric()
        else:
            monToDoJson = None
            monPub = None
            monThreads = None
            monErrors = None


# programm
init_log(download_logfile, download_loglevel, loggerName)
LOG.info(" ----- STARTED download.py -----")
if toPublish == "True":
    LOG.info("pub message broker is: " + str(pub_message_broker))
if download_flat == "True":
    LOG.info(" - flat download")
if download_toDoDir.endswith("/"):
    local_memory = download_toDoDir + "done/"
else:
    local_memory = download_toDoDir + "/done/"
pub_client = None
pub_connected_flag = False
numThreads = 0

if toPublish == "True":
    getPubConnection()

if os.path.isdir(inputDir):
    while True:
        # list of toDoFiles from input dir
        inputFiles = [f for f in os.listdir(
                                   inputDir) if os.path.isfile(
                                                  os.path.join(inputDir, f)) and not f.startswith('.')]
        LOG.info(" - inputFiles len is: " + str(len(inputFiles)))
        init_counter(msg_counter)
        readInput_timeNow = datetime.now()
        readInput_printDay = readInput_timeNow.strftime('%Y%m%d')
        if readInput_printDay in msg_counter.keys():
            msg_counter[readInput_printDay] = msg_counter[readInput_printDay] + len(inputFiles)
            LOG.info(" - msg_counter for day: " + readInput_printDay + " updated to: " + str(msg_counter[readInput_printDay]))
        else:
            LOG.error(" - MISSING day entry (" + readInput_printDay + " in msg_counter (keys are: " + str(msg_counter.keys()) + ")")
        if monitor_metrics == "True":
            monToDoJson.set(msg_counter[readInput_printDay])
        while len(inputFiles) > 0:
            LOG.info("New Downloads arrived: " + str(len(inputFiles)) + " files, number of active threads: " + str(numThreads))
            if numThreads < 100:
                for item in inputFiles:
                    try:
                        if ".nfs" not in item:
                            src_todo = os.path.join(inputDir, item)
                            Json_inWork_name = "." + item
                            Json_inWork = os.path.join(inputDir, Json_inWork_name)
                            shutil.move(src_todo, Json_inWork)
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
                            # read toDo json file
                            myFile = open(Json_inWork, "r")
                            filecontent = myFile.read()
                            myFile.close()
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
                                    if "topic" in json_file.keys():
                                        topic = json_file["topic"]
                                    else:
                                        LOG.info(" - missing value in JSON: \
                                         topic, set to ''")
                                    if "instance_identifier" in json_file.keys():
                                        data_id = json_file["instance_identifier"]
                                    else:
                                        if "data_id" in json_file.keys():
                                            data_id = json_file["data_id"]
                                        else:
                                            LOG.info(" - missing value in JSON: \
                                             data_id, set to ''")
                                    if "/" in data_id:
                                        dataId_dir = os.path.dirname(data_id)
                                    else:
                                        dataId_dir = ""
                                    if "/" in data_id:
                                        filename_data_id = data_id.replace(
                                                    "/",
                                                    dataId_replace)
                                    else:
                                        filename_data_id = data_id
                                    infoFile = local_memory + filename_data_id

                                    if (downloadFile == "" or sourceUrl == ""):
                                        LOG.error(" - jsonFile toDownload: \
                                          missing downloadFile \
                                          or sourceUrl, json is: "
                                          + str(filecontent))
                                        if monitor_metrics == "True":
                                            monErrors.inc()
                                    if download_flat == "True":
                                        targetDir = ""
                                    else:
                                        targetDir = os.path.dirname(downloadFile)
                            except Exception as e:
                                LOG.error(" - while reading json toDo, error was: " + str(e))
                                errorDir = inputDir + "/errors/"
                                errorFile = os.path.join(errorDir, item)
                                shutil.move(Json_inWork, errorFile)
                                if monitor_metrics == "True":
                                    monErrors.inc()
                            if targetDir == "":
                                errorDir = inputDir + "/errors/"
                            else:
                                errorDir = download_targetDir + "/errors/"
                            errorDir = errorDir.replace("//", "/")
                            if targetDir != "":
                                if not os.path.exists(targetDir):
                                    LOG.info(" - create targetDir: " + targetDir)
                                    os.makedirs(targetDir)
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
                                    monErrors.inc()
                                os.remove(Json_inWork)
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
                                                    monErrors.inc()
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
                                                        monErrors.inc()
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
                                                             pub_client)
                                                    except BaseException as err:
                                                        LOG.error("NOT reached: "
                                                          + sourceUrl)
                                                        LOG.error(err)
                                                        move_json2error(Json_inWork)
                                                        if monitor_metrics == "True":
                                                            monErrors.inc()
                                        else:
                                            LOG.error("MISSING: data_id in memory, \
                                              already downloaded")
                                            touch_memFile(local_memory,
                                                  data_id)
                                            # calculate and write integrity
                                            integrity_file(downloadFile,
                                                   infoFile,
                                                   integrity_method)
                                    else:
                                        alreadyThere = os.path.exists(infoFile)
                                        if alreadyThere is True:
                                            os.remove(Json_inWork)
                                            LOG.info("MISSING: integrity value, \
                                             already downloaded: "
                                             + downloadFile)
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
                                                          pub_client)
                                            except BaseException as err:
                                                LOG.error(" - createNewDownloadThread: "
                                                        + sourceUrl + " error was: " + str(err))
                                                move_json2error(Json_inWork)
                                                if monitor_metrics == "True":
                                                    monErrors.inc()
                                        else:
                                            LOG.info(" - download only core: "
                                             + str(topic))
                                            os.remove(Json_inWork)
                                            if monitor_metrics == "True":
                                                monErrors.inc()
                                    else:
                                        try:
                                            createNewDownloadThread(
                                                    sourceUrl,
                                                    downloadFile,
                                                    integrity,
                                                    integrity_method,
                                                    data_id,
                                                    Json_inWork,
                                                    pub_client)
                                        except BaseException as e:
                                            LOG.error(" - createNewDownloadThread: "
                                              + sourceUrl + " error was: " + str(e))
                                            move_json2error(Json_inWork)
                                            if monitor_metrics == "True":
                                                monErrors.inc()
                    except Exception as e:
                        LOG.error(e)
                    inputFiles.remove(item)
                    LOG.info(" - inputFiles reduced to: " + str(len(inputFiles)))
            else:
                LOG.warning(" - all threads in use")
                time.sleep(2)
        if len(inputFiles) == 0:
            LOG.info(" - WAITING for input files")
            time.sleep(2)
        else:
            LOG.info(" - input files count is: " + str(len(inputFiles)))
else:
    LOG.error(" - inputDir (toDoDir) not found")
