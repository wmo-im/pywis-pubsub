#!/usr/bin/python3
import paho.mqtt.client as mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import os.path
from pathlib import Path
import json
import time
from datetime import datetime
import argparse
# import ssl
import socket
import logging
from logging.handlers import RotatingFileHandler
import shutil
import requests
import uuid
from urllib.parse import urlparse
from websocket import create_connection
from prometheus_client import Counter, Gauge, start_http_server
from prometheus_client import Summary, REGISTRY, PROCESS_COLLECTOR, PLATFORM_COLLECTOR
import threading

# Args
parser = argparse.ArgumentParser(
             description='Subscribe to AMQPS message broker with config file',
             formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--config', default="", type=str, help='config file name')
args = parser.parse_args()

if args.config == "":
    print("Please use --config and config file name as argument")
    config_filename = ""
else:
    config_filename = args.config
    configFile = os.path.basename(config_filename)


# functions
def init_log(logFile, logLevel, myLoggerName):
    global LOG
    handlers = [RotatingFileHandler(filename=logFile,
                mode='a',
                maxBytes=512000,
                backupCount=2)]
    logging.basicConfig(handlers=handlers,
                        level=logLevel,
                        format='%(levelname)s %(asctime)s %(message)s',
                        datefmt='%Y%m%dT%H:%M:%S')
    LOG = logging.getLogger(myLoggerName)


def timeLag(myPubTime):
    timeNow_timeLag = datetime.now()
    if "Z" in myPubTime:
        myPubTime = myPubTime.replace("Z", "")
    if "z" in myPubTime:
        myPubTime = myPubTime.replace("z", "")
    if "-" in myPubTime:
        if len(myPubTime) > 19:
            LOG.debug(" - myPubTime reduced to 19 digits")
            myPubTime = myPubTime[0:19]
    else:
        if len(myPubTime) > 15:
            LOG.debug(" - myPubTime reduced to 15 digits")
            myPubTime = myPubTime[0:15]
    if "-" in myPubTime:
        pubTimeDate = datetime.strptime(myPubTime, "%Y-%m-%dT%H:%M:%S")
    else:
        pubTimeDate = datetime.strptime(myPubTime, "%Y%m%dT%H%M%S")
    if timeNow_timeLag > pubTimeDate:
        myTimeLag = timeNow_timeLag - pubTimeDate
        myTimeLagSec = myTimeLag.total_seconds()
        myTimeLagSec = round(myTimeLagSec, 2)
    else:
        myTimeLagSec = "now before pubTime"
    return myTimeLagSec


# aria2
def connect2aria2(myAria2_ws_url):
    count = 0
    while count < 4:
        try:
            myWebsocket = create_connection(
                myAria2_ws_url,
                http_no_proxy=myAria2_ws_url
            )
            count = 4
        except ConnectionRefusedError:
            time.sleep(2)
            myWebsocket = None
            count = count + 1
    return myWebsocket


def getAria2Status(my_gid):
    jsonreq_status = json.dumps({'jsonrpc': '2.0',
                                 'id': 'sub_client_wis2box_dwd',
                                 'method': 'aria2.tellStatus',
                                 'params': ['token:P3TERX',
                                            my_gid,
                                            ["status"]]})
    ws.send(jsonreq_status)
    LOG.info(" - tellStatus aria2 for gid: "
             + str(my_gid)
             + " is: " + str(jsonreq_status))


def listen4msg(myWebsocket):
    global listen4msg_started, watchlist_downloads, download_targetDir
    list_empty = False
    while not list_empty:
        message = myWebsocket.recv()
        listen4msg_started = True
        msg_json = json.loads(message)
        LOG.debug(msg_json)
        response_params = msg_json["params"][0]
        response_gid = response_params["gid"]
        if "aria2.onDownloadError" in msg_json["method"]:
            LOG.error(" - aria2.onDownloadError for gid: "
                      + str(watchlist_downloads[response_gid]))
            getAria2Status(response_gid)
            if response_gid in watchlist_downloads.keys():
                watchlist_downloads.pop(response_gid)
                count = len(watchlist_downloads.keys())
                LOG.debug(" - watchlist_downloads count: " + str(count))
                if count == 0:
                    list_empty = True
                    listen4msg_started = False
        if "aria2.onDownloadComplete" in msg_json["method"]:
            LOG.info(" - aria2.onDownloadComplete for gid: " + response_gid)
            if response_gid in watchlist_downloads.keys():
                # write empty file with data_id in name
                my_data_id = watchlist_downloads[response_gid]["data_id"]
                myFilename = my_data_id.replace("/", dataId_replace)
                myDataIDsDir = os.path.join(
                    download_targetDir,
                    dataID_infoFile_dir
                )
                if not os.path.exists(myDataIDsDir):
                    os.makedirs(myDataIDsDir)
                fullpath = os.path.join(
                    myDataIDsDir,
                    myFilename
                )
                dataIDFile = open(fullpath, "w")
                dataIDFile.write("\n")
                dataIDFile.close()
                LOG.info(" - aria2.onDownloadComplete for data_id: "
                         + str(my_data_id))
                # remove from watchlist_downloads
                watchlist_downloads.pop(response_gid)
                LOG.debug(" - deleted gid " + response_gid
                          + " from watchlist_downloads")
                # watchlist_downloads empty
                count = len(watchlist_downloads.keys())
                LOG.debug(" - watchlist_downloads count: " + str(count))
                if count == 0:
                    list_empty = True
                    listen4msg_started = False
            else:
                LOG.error(" - aria2.onDownloadComplete gid ("
                          + response_gid + ") missed in watchlist")


def send2aria(my_aria2_http_url, download_url, data_id):
    global listen4msg_started, watchlist_downloads, ws
    aria2_id = str(uuid.uuid1())
    jsonreq = json.dumps({'jsonrpc': '2.0',
                          'id': aria2_id,
                          'method': 'aria2.addUri',
                          'params': ['token:P3TERX',
                                     [download_url]]})
    aria2_downloadReqResponse = requests.post(my_aria2_http_url, jsonreq)
    aria2_downloadReqResult = aria2_downloadReqResponse.text
    LOG.debug(aria2_downloadReqResult)
    if "result" in aria2_downloadReqResult:
        aria2_downloadReqResult_json = json.loads(aria2_downloadReqResult)
        download_gid = aria2_downloadReqResult_json['result']
        LOG.info(" - start aria2 download with gid: " + str(download_gid))
        watchlist_item = {download_gid: {"data_id": data_id,
                                         "url": download_url}}
        watchlist_downloads.update(watchlist_item)
        LOG.debug(" - added new item to watchlist_downloads: "
                  + str(watchlist_item))
        LOG.debug(" - listen4msg_started is: " + str(listen4msg_started))
        if not listen4msg_started:
            listen4msg(ws)
    else:
        LOG.error(" - send2aria error result: " + str(aria2_downloadReqResult))
        LOG.error(" - send2aria url: " + download_url)
        LOG.error(" - send2aria data_id: " + data_id)


# python download
def write_toDownload(msg_id, url,
                     data_identifier,
                     topic, integrity,
                     integrity_method,
                     content_encoding,
                     content_value):
    global monitor_metrics, monMsgContent, monToDoDuplicate, monMsgCountryErr, monToDo, numThreads
    numThreads = numThreads + 1
    fname = "missingFilename"
    if download_orgFilename == "True":
        myURL = urlparse(url)
        fname_identifer = os.path.basename(myURL.path)
    else:
        fname_identifer = data_identifier.replace("/", "__")
        fname_identifer = fname_identifer.replace(":", "")
    fname = fname_identifer
    LOG.debug("info - download filename is: " + fname)
    topicSubtree = topic.replace('.', '/')
    if download_flat == "True":
        targetDir = download_targetDir
    else:
        targetDir = download_targetDir + topicSubtree
    if not os.path.exists(targetDir):
        os.makedirs(targetDir, exist_ok=True)
    downloadFile = (targetDir + '/' + fname).replace('//', '/')
    toDownloadFile = fname.split(".")[0] + ".json"
    toDownloadDir_path = Path(download_toDoDir)
    if not os.path.exists(download_toDoDir):
        toDownloadDir_path.mkdir(parents=True)
    if content_encoding != "" and content_value != "":
        fileContent = '{"topic":"' + topic \
                          + '", "content_encoding":"' + content_encoding \
                          + '", "content_value":"' + content_value \
                          + '", "msg_id":"' + str(msg_id) + '"}'
    else:
        fileContent = '{"topic":"' + topic \
                          + '", "downloadFilename":"' + downloadFile \
                          + '", "integrity":"' + integrity \
                          + '", "integrity_method":"' + integrity_method \
                          + '", "sourceUrl": "' + url \
                          + '", "data_id":"' + str(data_identifier) + '"}'
    fileContentJSON = json.loads(fileContent)
    if "content_value" in fileContentJSON.keys():
        toDownloadFile = "msgContent_" + printTimeNow + ".json"
        newDownload = open(os.path.join(download_toDoDir,
                                        toDownloadFile), "w")
        newDownload.write(json.dumps(fileContentJSON, indent=4))
        newDownload.close()
        if monitor_metrics == "True":
            monMsgContent.inc()
    else:
        if (fileContentJSON["downloadFilename"] != "" and
           fileContentJSON["downloadFilename"] is not None and
           fileContentJSON["sourceUrl"] != "" and
           fileContentJSON["sourceUrl"] is not None):
            json_already_there = os.path.exists(os.path.join(
                                                download_toDoDir,
                                                toDownloadFile))
            if json_already_there:
                monToDoDuplicate.labels("ALL").inc() 
                LOG.error(" - json already written, message msg_id is: " 
                        + str(msg_id) + ", data_id: " + str(data_identifier))
            newDownload = open(os.path.join(download_toDoDir,
                                            toDownloadFile), "w")
            newDownload.write(json.dumps(fileContentJSON, indent=4))
            if monitor_metrics == "True":
                monToDo.inc()
            newDownload.close()
        else:
            LOG.error(" - missing downloadFilename\
                       for message ('"
                      + str(msg_id) + "') of topic: " + topic)
            if monitor_metrics == "True":
                if "swe/smhi" in topic:
                    monMsgCountryErr.labels("SWEDEN").inc()
                monMsgCountryErr.labels("ALL").inc()
    if numThreads > 0:
        numThreads = numThreads - 1


# added to check if data_id in local memory - already downloaded
def alreadyDownloaded(my_data_id, my_topic):
    global local_memory
    global localMem_integrity
    global monitor_metrics, monMsgCountryDuplicate
    already_downloaded = False
    if "/" in my_data_id:
        my_data_id = my_data_id.replace("/", dataId_replace)
    memory_file = os.path.join(local_memory, my_topic, my_data_id)
    already_downloaded = os.path.exists(memory_file)
    if already_downloaded:
        with open(memory_file, "r") as myCacheFile:
            localMem_integrity = myCacheFile.read()
        if monitor_metrics == "True":
            if "swe/smhi" in my_topic:
                monMsgCountryDuplicate.labels("SWEDEN").inc()
            monMsgCountryDuplicate.labels("ALL").inc()
    return already_downloaded


# checks before writing JSON for toDownload
def checkBeforeWriteDownload(my_data_id, msg_integrity, my_topic, my_url):
    global local_memory, writeJsonToDownload, monMsgCountryErr, monitor_metrics
    writeJsonToDownload = False
    alreadyInMem = False
    # missing dat_identifier
    if my_data_id == "":
        LOG.error(" - missing data_identifer in message, \
                  toDownload NOT written")
        if monitor_metrics == "True":
            if "swe/smhi" in my_topic:
                monMsgCountryErr.labels("SWEDEN").inc()
            monMsgCountryErr.labels("ALL").inc()
    else:
        if my_url == "":
            LOG.error(" - missing url in message, NO toDownload written")
            if monitor_metrics == "True":
                if "swe/smhi" in my_topic:
                    monMsgCountryErr.labels("SWEDEN").inc()
                monMsgCountryErr.labels("ALL").inc()
        else:
            alreadyInMem = alreadyDownloaded(my_data_id, my_topic)
            if alreadyInMem:
                if msg_integrity == localMem_integrity:
                    writeJsonToDownload = False
                    LOG.info(" - msg already downloaded")
                else:
                    writeJsonToDownload = True
            else:
                writeJsonToDownload = True
            LOG.debug(" - writeJsonToDownload value \
                      before whitelist check is: "
                      + str(writeJsonToDownload))
            # whitelist check
            if writeJsonToDownload is True:
                inWhitelist = False
                if download_whitelist != "":
                    for whitelist_item in myWhitelist:
                        if whitelist_item in my_url:
                            inWhitelist = True
                else:
                    # no whitelist value in config file,
                    # download from everywhere
                    inWhitelist = True
                if not inWhitelist:
                    LOG.error(" - domain of " + my_url
                              + " NOT in configured whitelist")
                    writeJsonToDownload = False
                    if monitor_metrics == "True":
                        if "swe/smhi" in my_topic:
                            monMsgCountryErr.labels("SWEDEN").inc()
                        monMsgCountryErr.labels("ALL").inc()


def readMSG(MSG, myTopic):
    global monitor_metrics, monCountry
    showTimeLag = "False"
    if MSG != "":
        # pubTime and timeLag
        if "properties" in MSG.keys():
            msg_pubtime = ""
            if "publication_datetime" in MSG["properties"].keys():
                msg_pubtime = MSG["properties"]["publication_datetime"]
            if "pub_datetime" in MSG["properties"].keys():
                msg_pubtime = MSG["properties"]["pub_datetime"]
            if "pubTime" in MSG["properties"].keys():
                msg_pubtime = MSG["properties"]["pubTime"]
            if "pubtime" in MSG["properties"].keys():
                msg_pubtime = MSG["properties"]["pubtime"]
            if msg_pubtime != "":
                time_lag = timeLag(msg_pubtime)
                showTimeLag = "True"
                # publication_datetime = msg_pubtime
            else:
                LOG.error(" - no pubtime in message: "
                          + str(json.dumps(MSG, indent=4)))
        # get url for data download
        url = ""
        if "links" in MSG["properties"].keys():
            urlList = MSG["properties"]["links"]
        else:
            if "links" in MSG.keys():
                urlList = MSG["links"]
            else:
                LOG.error(" - no links in message: "
                          + str(json.dumps(MSG, indent=4)))
                urlList = None

        if urlList is not None:
            for item in urlList:
                if item["rel"] == "canonical":
                    url = item["href"]
            if url == "":
                LOG.info(" - no canonical href in links"
                         + " use first href value instead")
                url = urlList[0]["href"]
        if url == "":
            LOG.error(" - no links in message: "
                      + str(json.dumps(MSG, indent=4)))
        # integrity
        integrity = ""
        if "integrity" in MSG["properties"].keys():
            integrity_method = MSG["properties"]["integrity"]["method"]
            integrity = MSG["properties"]["integrity"]["value"]
        else:
            integrity_method = ""
        if integrity_method != "":
            if (integrity_method != "md5" and
               integrity_method != "MD5" and
               integrity_method != "sha256" and
               integrity_method != "SHA256" and
               integrity_method != "sha512" and
               integrity_method != "SHA512"):
                LOG.error(" - integrity_method in message neither md5 \
                          nor sha256 nor sha512. Integrity_method: "
                          + integrity_method)
        # content included im msg
        if "content" in MSG["properties"].keys():
            if "encoding" in MSG["properties"]["content"].keys():
                content_encoding = MSG["properties"]["content"]["encoding"]
            else:
                LOG.warning(" - MISSING encoding in message/properties/content")
                content_encoding = ""
            if "value" in MSG["properties"]["content"].keys():
                content_value = MSG["properties"]["content"]["value"].replace(
                                                                    "\n", "")
            else:
                LOG.warning(" - MISSING value in message/properties/content")
                content_value = ""
        else:
            content_encoding = ""
            content_value = ""
        # data_id
        data_identifier = ""
        if "data_id" in MSG["properties"]:
            data_identifier = MSG["properties"]["data_id"]
            data_identifier = data_identifier.replace("//", "/")
        else:
            if "instance_identifier" in MSG["properties"]:
                data_identifier = MSG["properties"]["instance_identifier"]
                data_identifier = data_identifier.replace("//", "/")
        # msg_id
        msg_id = str(MSG["id"])
        # write msg to log
        LOG.debug(" - message topic:        " + str(myTopic))
        LOG.debug(" - message data id:      " + str(data_identifier))
        LOG.debug(" - message id:           " + msg_id)
        if showTimeLag == "True":
            LOG.debug(" - message time lag:     " + str(time_lag) + "[sec]")
        LOG.debug(" - message url:          " + str(url))
        LOG.debug(" - message:            " + str(json.dumps(MSG, indent=4)))
        if show_message == "True":
            print("New message:     " + str(json.dumps(MSG, indent=4)))
        # msg_store
        if msg_store is not None and msg_store != "":
            LOG.info(" - write MSG to msg_store...")
            if data_identifier == "":
                fname = str(msg_id)
                LOG.error(" - MISSING data_id in message: "
                          + str(msg_id))
            else:
                fname = data_identifier.replace("/", "___")
            toFilename = msg_store + str(fname)
            msgStore_path = Path(msg_store)
            if not os.path.exists(msg_store):
                msgStore_path.mkdir(parents=True)
            toFile = open(toFilename, "w")
            toFile.write(str(json.dumps(MSG, indent=4)))
            toFile.close()
        # write JSON for toDownload
        if withDownload == "True":
            LOG.debug(" - starting checks before download")
            checkBeforeWriteDownload(data_identifier, integrity, myTopic, url)
            if writeJsonToDownload is True:
                if useAria2 == "True":
                    LOG.info(" - start download, send to arai2 url: "
                             + str(url))
                    send2aria(aria2_http_url, url, str(data_identifier))
                else:
                    LOG.info(" - write JSON for toDownload...")
                    if numThreads < 100:
                        createNewWriteJsonThread(msg_id, url, data_identifier,
                                     myTopic, integrity, integrity_method,
                                     content_encoding, content_value)
                        if monitor_metrics == "True":
                            if "swe/smhi" in myTopic:
                                monCountry.labels("SWEDEN").inc()
                    else:
                        LOG.warning(" - all threads for write json in use")
                        time.sleep(2)


def createNewWriteJsonThread(msg_id, url,
                             data_identifier,
                             topic, integrity,
                             integrity_method,
                             content_encoding,
                             content_value):
    global monitor_metrics, monMsgContent, monToDoDuplicate, monMsgCountryErr, monToDo
    if (url == "" or msg_id == "" or data_identifier == "" or topic == ""):
        LOG.error(" - createNewWriteJsonThread: missing url, \
                  msg_id, topic or data_id value")
        LOG.error(" - url value is: " + str(url)
                  + " /  msg_id is: "
                  + str(msg_id)
                  + " / data_id is: " + str(data_identifier)
                  + " / topic is: " + str(topic))
    else:
        writeJson_thread = threading.Thread(
                                 target=write_toDownload,
                                 args=(
                                    msg_id, url,
                                    data_identifier,
                                    topic, integrity,
                                    integrity_method,
                                    content_encoding,
                                    content_value))
        writeJson_thread.start()


# message received
def on_mqtt_message(client, userdata, message):
    global integrity_method, download_targetDir, ws, aria2_http_url, monCountry, monMsgCountryErr
    LOG.info("---- NEW MESSAGE ----")
    try:
        topic = message.topic
        msg = json.loads(message.payload.decode("utf-8"))
#       validate(instance=msg, schema=schema)
#       LOG.debug("validated msg: " + str(topic))
#   except jsonschema.exceptions.ValidationError as err:
#       LOG.error("validation error occured for msg: "
#                 + message.payload.decode("utf-8"))
#       LOG.error(err)
        if monitor_metrics == "True":
            monCountry.labels("ALL").inc()
        readMSG(msg, topic)
    except Exception as e:
        msg = ""
        LOG.error(" - json loads error occured for msg: "
                  + message.payload.decode("utf-8"))
        LOG.error(" - on_mqtt_message payload error: " + str(e))
        if monitor_metrics == "True":
            monMsgCountryErr.labels("ALL").inc()


# connect to message broker
def on_connect(client, userdata, flags, rc, properties=None):
    global Connected
    LOG.info(" - on_connect code is: " + str(rc))
    if rc == 0:
        Connected[topicCounter][clientIndex] = True
        LOG.debug("Connected_flag value for client is: "
                  + str(Connected[topicCounter][clientIndex]))
        result = client.subscribe(sub_topic, qos=1,
                                  options=None, properties=None)
        if result[0] == 0:
            LOG.info(" - subscribed to topic: "
                     + str(sub_topic) + " as "
                     + group_clientname)
        else:
            LOG.error(" - connection failed with result code: " + str(rc))
            client.disconnect()


def on_connect_wait(myclient):
    global toBeClosed
    for x in range(5):
        time.sleep(2)
        if Connected[topicCounter][clientIndex] is False:
            LOG.info(" - in wait loop to connect client")
    if Connected[topicCounter][clientIndex] is False:
        LOG.error(" - no connect for sub possible")
        toBeClosed = True
        myclient.disconnect()
        myclient.loop_stop()


# disconnect from message broker
def on_disconnect(client, userdata, rc, properties=None):
    global Subscribed
    global Connected
    Connected[topicCounter][client] = False
    Subscribed = False
    client.loop_stop()

def connect_prometheus(prometheus_port):
    try:
        start_http_server(prometheus_port)
    except:
        print("Can not listen on " + str(prometheus_port))


def init_metric():
    global monCountry, monMsgCountryErr, monToDo, monMsgCountryDuplicate, monMsgContent, monToDoDuplicate
    connect_prometheus(PROMETHEUS_PORT)
    REGISTRY.unregister(PROCESS_COLLECTOR)
    REGISTRY.unregister(PLATFORM_COLLECTOR)
    REGISTRY.unregister(REGISTRY._names_to_collectors['python_gc_objects_collected_total'])
    
    monCountry = Gauge('msg_received', 'Amount of messages received from Country',["Country"])
    monCountry.labels("SWEDEN").set(0)
    monCountry.labels("ALL").set(0)

    monMsgCountryErr = Gauge('msg_received_errors', 'Amount of messages received from Country with errors',["Country"])
    monMsgCountryErr.labels("SWEDEN").set(0)
    monMsgCountryErr.labels("ALL").set(0)

    monMsgCountryDuplicate = Gauge('msg_received_duplicate', 'Amount of messages received from Country as duplicate',["Country"])
    monMsgCountryDuplicate.labels("SWEDEN").set(0)
    monMsgCountryDuplicate.labels("ALL").set(0)

    monToDo = Gauge('msg_toDoJson_written', 'Amount of written toDo json files')
    monToDo.set(0)

    monToDoDuplicate = Gauge('msg_toDoJson_duplicate', 'Amount of NOT written Json files as they are duplicates',["Country"])
    monToDoDuplicate.labels("ALL").set(0)

    monMsgContent = Gauge('msg_received_withcontent', 'Amount of messages received with content in message')
    monMsgContent.set(0)

# declaration
channel_closed = True
connection_closed = True
toBeClosed = False
Connected = {}
topicCounter = 0
Subscribed = False
integrity_method = ""
content_encoding = ""
content_value = ""
reconnect_count = 1
sub_topic_array = []
timeNow = datetime.now()
printTimeNow = timeNow.strftime('%Y%m%dT%H%M%S')
configFile = configFile.replace(".json", "")
loggerName = "subscribe" + str(configFile)
LOG = None
writeJsonToDownload = False
localMem_integrity = ""
# if data_id includes '/' it should be replaced by...
dataId_replace = "___"
ws = None
dataID_infoFile_dir = "dataIDs/"
aria2_ws_url = "ws://aria2-pro:6800/jsonrpc"
aria2_http_url = "http://aria2-pro:6800/jsonrpc"
watchlist_downloads = json.loads('{}')
listen4msg_started = False
group_clientname = ""
numThreads = 0

# read config file values
if config_filename == "":
    print("error - config file: no config file.")
else:
    # read config
    with open(config_filename, 'r') as myConfigFile:
        data = myConfigFile.read()
    myConfig = json.loads(data)

    # subscribe
    if "toSubscribe" in myConfig.keys():
        toSubscribe = myConfig["toSubscribe"]
    else:
        print("error - config file - missing value: toSubscribe, set to False")
        toSubscribe = "False"
    if toSubscribe == "True":
        if "wis2box" in myConfig.keys():
            wis2box = myConfig["wis2box"]
        else:
            print("error -  config file - missing value: \
                  wis2box, set to False")
            wis2box = "False"
        if "sub_protocol" in myConfig.keys() and \
           "sub_host" in myConfig.keys() and \
           "sub_port" in myConfig.keys() and \
           "sub_user" in myConfig.keys() and \
           "sub_password" in myConfig.keys():
            sub_protocol = myConfig["sub_protocol"]
            sub_host = myConfig["sub_host"]
            sub_port = myConfig["sub_port"]
            sub_user = myConfig["sub_user"]
            sub_password = myConfig["sub_password"]
        else:
            print("error - config file - missing value: \
                  sub_protocol, sub_host, sub_port, \
                  sub_user and/or sub_password")

        if "sub_logfile" in myConfig.keys():
            sub_logfile = myConfig["sub_logfile"]
        else:
            print("error - config file - missing value: \
                  sub_logfile, set to sub_connect_datetime.log")
            sub_logfile = "sub_connect_" + printTimeNow + ".log"
        if "sub_loglevel" in myConfig.keys():
            sub_loglevel = myConfig["sub_loglevel"]
        else:
            print("error - config file - missing value: \
                  sub_loglevel, set to INFO.")
            sub_loglevel = "INFO"

        if sub_protocol == "amqps" or sub_protocol == "amqp":
            print("error - use amqp sub script, \
                  this script is only for mqtt(s).")
        else:
            if sub_protocol == "mqtts" or sub_protocol == "mqtt":
                if sub_protocol == "mqtts":
                    if "sub_cacert" in myConfig.keys():
                        sub_cacert = myConfig["sub_cacert"]
                    else:
                        if wis2box == "True":
                            source_cacert_dir = "/usr/src/sub/caFiles/"
                            target_cacert_dir = "/usr/src/sub/caFiles/"
                            cafile_crt = ""
                            cafile_pem = ""
                            for cafile in os.listdir(source_cacert_dir):
                                if os.path.isfile(
                                             os.path.join(source_cacert_dir,
                                                          cafile)):
                                    if ".crt" in cafile:
                                        cafile_crt = cafile
                                    if ".pem" in cafile:
                                        if "tls" in cafile:
                                            cafile_pem = cafile
                            if cafile_crt == "":
                                if cafile_pem != "":
                                    shutil.copyfile(
                                              os.path.join(source_cacert_dir,
                                                           cafile_pem),
                                              os.path.join(target_cacert_dir,
                                                           "ca-bundle.crt"))
                                    sub_cacert = os.path.join(
                                                       target_cacert_dir,
                                                       "ca-bundle.crt")
                            else:
                                shutil.copyfile(
                                       os.path.join(source_cacert_dir,
                                                    cafile_crt),
                                       os.path.join(target_cacert_dir,
                                                    cafile_crt))
                                sub_cacert = os.path.join(target_cacert_dir,
                                                          cafile_crt)
                        else:
                            print("error - config file - missing value: \
                                  sub_cacert")
                            sub_cacert = ""
                if "sub_topic" in myConfig.keys():
                    sub_topic_config = myConfig["sub_topic"]
                else:
                    print("error - config file - missing value: \
                          sub_topic, set to '#'")
                    sub_topic_config = ['#']
                if "sub_maxMSGsize" in myConfig.keys():
                    sub_maxMSGsize = myConfig["sub_maxMSGsize"]
                else:
                    print("error - config file - missing value: \
                          sub_maxMSGsize, set to 2048")
                    sub_maxMSGsize = 2048
                sub_clientname = socket.gethostname()
                if "sub_clientname" in myConfig.keys():
                    if myConfig["sub_clientname"] != "":
                        sub_clientname = sub_clientname + "_" \
                                         + myConfig["sub_clientname"]
                else:
                    print("error - config file - missing value:  \
                          sub_clientname, set to hostname")
                if "sub_protocol_version" in myConfig.keys():
                    sub_protocol_version = myConfig["sub_protocol_version"]
                else:
                    print("error - config file - missing value: \
                          sub_protocol_version, set to MQTTv5")
                    sub_protocol_version = "MQTTv5"
                sub_brokerAddress = sub_protocol + "://" \
                    + sub_user + ":" + sub_password \
                    + "@" + sub_host + ":" + sub_port
                message_broker = sub_protocol + "://" \
                    + sub_user + ":[passwd]@" \
                    + sub_host + ":" + sub_port

                if "show_message" in myConfig.keys():
                    show_message = myConfig["show_message"]
                else:
                    print("error - config file - missing value: \
                          show_message, set to False")
                    show_message = "False"
                if "msg_store" in myConfig.keys():
                    msg_store = myConfig["msg_store"]
                else:
                    print("error - config file - missing value: \
                          msg_store, set to None")
                    msg_store = None
                if "sub_share_name" in myConfig.keys():
                    sub_share_name = myConfig["sub_share_name"]
                else:
                    print("error - config file - missing value: \
                          sub_share_name, set to ''")
                    sub_share_name = ""
                if "sub_share_name" != "" and sub_protocol_version == "MQTTv5":
                    for item in sub_topic_config:
                        new_topic = "$share/" + sub_share_name + "/" + item
                        sub_topic_array.append(new_topic)
                else:
                    sub_topic_array = sub_topic_config
                if "sub_share_quantity" in myConfig.keys():
                    sub_share_quantity = myConfig["sub_share_quantity"]
                else:
                    print("error - config file - missing value: \
                          sub_share_quantity, set to 1")
                    sub_share_quantity = 1

        # download
        download_whitelist = ""
        if "useAria2" in myConfig.keys():
            useAria2 = myConfig["useAria2"]
        else:
            print("error -  config file - missing value: \
                  useAria2, set to False")
            useAria2 = "False"
        if "withDownload" in myConfig.keys():
            withDownload = myConfig["withDownload"]
        else:
            print("error -  config file - missing value: \
                  withDownload, set to False")
            withDownload = "False"
        if withDownload == "True":
            if "download_toDoDir" in myConfig.keys():
                download_toDoDir = myConfig["download_toDoDir"]
            else:
                print("error -  config file - missing value: \
                      download_toDoDir, set to local toDownload")
                download_toDoDir = "toDownload/"
            if not download_toDoDir.endswith("/"):
                download_toDoDir = download_toDoDir + "/"
            if "download_targetDir" in myConfig.keys():
                download_targetDir = myConfig["download_targetDir"]
            else:
                print("error -  config file - missing value: \
                      download_targetDir, set to local targetDownload")
                download_targetDir = "targetDownload/"
            if "filedownloadURL" in myConfig.keys():
                filedownloadURL = myConfig["filedownloadURL"]
            else:
                print("error -  config file - missing value: \
                      filedownloadURL, set to True")
                filedownloadURL = "True"
            if "download_flat" in myConfig.keys():
                download_flat = myConfig["download_flat"]
            else:
                print("error -  config file - missing value: \
                      download_flat, set to False \
                      - topic value is used as subtree")
                download_flat = "False"
            if "download_orgFilename" in myConfig.keys():
                download_orgFilename = myConfig["download_orgFilename"]
            else:
                print("error -  config file - missing value: \
                      download_orgFilename, set to True")
                download_orgFilename = "True"
            if "download_whitelist" in myConfig.keys():
                download_whitelist = myConfig["download_whitelist"]
            else:
                print("error -  config file - missing value: \
                      download_whitelist, set to ''")
                download_whitelist = ""

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
            else:
                print("error -  config file - missing value: \
                      PROMETHEUS_PORT, set to 12000")
                PROMETHEUS_PORT=12000
            init_metric()
        else:
            monMsgCountryErr = None
            monCountry = None
            monToDo = None
            monToDoDuplicate = None
            monMsgCountryDuplicate = None
            monMsgContent = None


myWhitelist = []
if download_whitelist != "":
    with open(download_whitelist) as fp:
        for line in fp:
            line = line.replace("\n", "")
            if line not in myWhitelist:
                myWhitelist.append(line)

# programm
if withDownload == "True":
    local_memory = download_toDoDir + "/done/"
else:
    local_memory = "done/"
logDir = os.path.dirname(sub_logfile)
logDir_path = Path(logDir)
if not os.path.exists(logDir):
    logDir_path.mkdir(parents=True)
init_log(sub_logfile, sub_loglevel, loggerName)
# schema = json.load(
#                open(
#                   "/home/gisc/scripts/withConfig/one4all/message-schema.json"))

LOG.info("##### script started #####")
LOG.info("##########################")
for item in myWhitelist:
    LOG.info(" - whitelist contains: " + item)
if myWhitelist == []:
    LOG.info(" - no whitelist value in config file, download from everywhere")

if withDownload == "True":
    if useAria2 == "True":
        LOG.info(" - download via ARIA2 (useAria2 is 'True' in config file)")
        if ws is None:
            ws = connect2aria2(aria2_ws_url)
    else:
        LOG.info(" - download via download.py (useAria2 is 'False')")
        LOG.info(" - please start download.py via start_py4download.sh")

if toSubscribe == "True":
    topic_clients = {}
    if sub_protocol == "mqtts" or sub_protocol == "mqtt":
        if sub_protocol_version == "MQTTv5":
            sub_properties = Properties(PacketTypes.CONNECT)
            sub_properties.MaximumPacketSize = sub_maxMSGsize
        else:
            sub_properties = None
        # new clients for each topic value
        for sub_topic in sub_topic_array:
            print("starting clients for topic: " + str(sub_topic))
            # mqttv5 shared subscription
            topic_clients[topicCounter] = {}
            Connected[topicCounter] = {}
            for clientCounter in range(int(sub_share_quantity)):
                suffix_client = uuid.uuid1()
                group_clientname = sub_clientname + "_" + str(suffix_client)
                clientIndex = clientCounter - 1
                if sub_protocol_version == "MQTTv5":
                    topic_clients[topicCounter][clientIndex] = mqtt.Client(
                                                    group_clientname,
                                                    protocol=mqtt.MQTTv5)
                else:
                    topic_clients[topicCounter][clientIndex] = mqtt.Client(
                                                    group_clientname,
                                                    protocol=mqtt.MQTTv311)
                topic_clients[topicCounter][clientIndex].username_pw_set(
                                                    sub_user,
                                                    password=sub_password)
                if sub_protocol == "mqtts":
                    if sub_cacert != "":
                        topic_clients[topicCounter][clientIndex].tls_set(
                                                    ca_certs=sub_cacert)
                    else:
                        topic_clients[topicCounter][clientIndex].tls_set(
                                                    tls_version=2)
                    # if sub_host == "localhost" or sub_host == "127.0.0.1":
                    # topic_clients[topicCounter][clientIndex].tls_insecure_set(True)
                Connected[topicCounter][clientIndex] = False
                topic_clients[topicCounter][clientIndex].on_message = on_mqtt_message
                topic_clients[topicCounter][clientIndex].on_connect = on_connect
                topic_clients[topicCounter][clientIndex].on_disconnect = on_disconnect
                LOG.info(" - starting a new client for topic ("
                         + str(sub_topic)
                         + "), counter value is: "
                         + str(clientCounter))
                topic_clients[topicCounter][clientIndex].connect(
                                                  sub_host,
                                                  port=int(sub_port),
                                                  properties=sub_properties)
                topic_clients[topicCounter][clientIndex].loop_start()
                time.sleep(2)
                if Connected[topicCounter][clientIndex] is False:
                    on_connect_wait(topic_clients[topicCounter][clientIndex])
                else:
                    LOG.info(" - connected for sub")
            topicCounter = topicCounter + 1

        # all clients still connected check
        try:
            while True:
                time.sleep(2)
                if not toBeClosed:
                    topicCounter = 0
                    for key in topic_clients:
                        for clientCounter in range(int(sub_share_quantity)):
                            clientIndex = clientCounter - 1
                            if Connected[topicCounter][clientIndex] is False:
                                LOG.error(" - lost connection for topic "
                                          + str(topic_clients[topicCounter])
                                          + " client number "
                                          + str(clientIndex)
                                          + " ...try to reconnect")
                                topic_clients[topicCounter][clientIndex].connect(
                                                   sub_host,
                                                   port=int(sub_port),
                                                   properties=sub_properties)
                                topic_clients[topicCounter][clientIndex].loop_start()
                                time.sleep(2)

                            if Connected[topicCounter][clientIndex] is False:
                                on_connect_wait(
                                     topic_clients[topicCounter][clientIndex])
                        topicCounter = topicCounter + 1
        except KeyboardInterrupt:
            print("info - exiting")
            topicCounter = 0
            for key in topic_clients:
                for clientCounter in range(int(sub_share_quantity)):
                    topic_clients[topicCounter][clientIndex].loop_stop()
                    topic_clients[topicCounter][clientIndex].disconnect()
                    topicCounter = topicCounter + 1
