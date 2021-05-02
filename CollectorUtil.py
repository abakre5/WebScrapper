import os
import pickle
import time
import traceback

import mysql.connector
import requests
from selenium.common.exceptions import TimeoutException


def fetchURL(browser, url, log):
    retry = 3
    err = ""
    while retry > 0:
        try:
            browser.get(url)
            time.sleep(1)
            break
        except Exception as e:
            retry -= 1
            err = e
    if retry == 0:
        log.error("Error occurred fetching URL: {} => ".format(url, err))
        traceback.print_stack()
        raise Exception("Error occurred fetching URL: {} => ".format(url, err))


def getImageCollectionURL(browser, index, url, log):
    retry = 3
    error = ""
    while retry > 0:
        try:
            urlPage = url + "/entries/" + str(index)
            browser.get(urlPage)
            log.info("Hitting URL {}".format(urlPage))
            cookieFile = open("../Cookie/Cookie.pkl", "rb")
            for cookie in pickle.load(cookieFile):
                browser.add_cookie(cookie)
            browser.maximize_window()
            cookieFile.close()
            break
        except TimeoutException as err:
            log.info("Retrying Image URL {}".format(url))
            time.sleep(15)
            retry -= 1
            error = err
    if retry < 1:
        log.error("Error occurred while parsing url => {}".format(str(url)), str(error))
        traceback.print_stack()
        raise Exception("Error occurred while parsing contest url {}".format(str(url)), str(error))


def getDBConnection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="1234",
        database="contestDB"
    )


def getCollectorArgList(valueList):
    return ','.join(map(str, valueList))


def insertCollectorContestImageInfo(databaseConnection, ContestData, log):
    cursor = databaseConnection.cursor()
    CHECK_IF_IMAGE_INFO_EXIST = "SELECT * FROM submitImageInfo WHERE CONTESTID = (%s) AND ENTRYID = (%s) LIMIT 1"
    INSERT_IMAGE_INFO = "INSERT INTO submitImageInfo VALUES {}"
    INSERT_ELIMINATE = "INSERT IGNORE INTO eliminated (CONTESTID, ENTRYNUM) VALUES {}"
    INSERT_RATED = "INSERT IGNORE INTO rated (CONTESTID, ENTRYNUM, RATING) VALUES {}"
    INSERT_WINNER = "INSERT IGNORE INTO winner (CONTESTID, ENTRYNUM) VALUES {}"
    INSERT_WITHDRAWN = "INSERT IGNORE INTO withdrawn (CONTESTID, ENTRYNUM) VALUES {}"
    INSERT_DELETED = "INSERT IGNORE INTO deleted (CONTESTID, ENTRYNUM) VALUES {}"
    insertEle = []
    eliminateEle = []
    ratedEle = []
    winnerEle = []
    withdrawnEle = []
    deletedEle = []
    for obj in ContestData:
        cursor.execute(CHECK_IF_IMAGE_INFO_EXIST, (obj['ContestId'], obj['EntryId']))
        result = cursor.fetchone()
        if result is None:
            insertEle.append(
                (obj['ContestId'], obj['ImageURL'], obj['EntryId'], obj['OwnerProfileUrl'], obj['LastUpdated']))
        else:
            upd = "UPDATE submitImageInfo SET LastUpdated = '" + str(
                obj['LastUpdated']) + "' WHERE ContestId = " + str(
                obj['ContestId']) + " AND EntryId = " + obj['EntryId']
            # cursor.execute(upd)
            executeStatement(log, cursor, upd)

        if obj['Eliminated'] == 'True':
            eliminateEle.append((obj['ContestId'], obj['EntryId']))
        if obj['Rating'] != 'False':
            ratedEle.append((obj['ContestId'], obj['EntryId'], obj['Rating']))
        if obj['Withdrawn'] == 'True':
            withdrawnEle.append((obj['ContestId'], obj['EntryId']))
        if obj['Winner'] == 'True':
            winnerEle.append((obj['ContestId'], obj['EntryId']))
        if obj['Deleted'] == 'True':
            winnerEle.append((obj['ContestId'], obj['EntryId']))

    if insertEle:
        args = getCollectorArgList(insertEle)
        # cursor.execute(INSERT_IMAGE_INFO.format(args))
        executeStatement(log, cursor, INSERT_IMAGE_INFO.format(args))
    if eliminateEle:
        args = getCollectorArgList(eliminateEle)
        # cursor.execute(INSERT_ELIMINATE.format(args))
        executeStatement(log, cursor, INSERT_ELIMINATE.format(args))
    if withdrawnEle:
        args = getCollectorArgList(withdrawnEle)
        # cursor.execute(INSERT_WITHDRAWN.format(args))
        executeStatement(log, cursor, INSERT_WITHDRAWN.format(args))
    if winnerEle:
        args = getCollectorArgList(winnerEle)
        # cursor.execute((INSERT_WINNER.format(args)))
        executeStatement(log, cursor, INSERT_WINNER.format(args))
    if deletedEle:
        args = getCollectorArgList(deletedEle)
        # cursor.execute((INSERT_DELETED.format(args)))
        executeStatement(log, cursor, INSERT_DELETED.format(args))
    if ratedEle:
        args = getCollectorArgList(ratedEle)
        # cursor.execute((INSERT_RATED.format(args)))
        executeStatement(log, cursor, INSERT_RATED.format(args))
    cursor.close()


def insertCollectorDesigner(databaseConnection, designerURL):
    valueList = []
    for dURL in designerURL:
        valueList.append('("' + dURL + '")')
    if valueList:
        cursor = databaseConnection.cursor()
        insertQuery = "INSERT IGNORE INTO designerURLs VALUES {}"
        urls = getCollectorArgList(valueList)
        cursor.execute(insertQuery.format(urls))
        cursor.close()


def getCollectorContestFinalParsingStatus(databaseConnection, contestID, log):
    try:
        cursor = databaseConnection.cursor()
        cursor.execute("SELECT STATUS, FINALPARSINGDONE FROM CONTESTDATA WHERE CONTESTID = " + str(contestID))
        result = cursor.fetchone()
        cursor.close()
        return result
    except Exception as err:
        log.error(
            "Contest {} not found in the contestdata table. Please run ContestInfoCollector.py".format(str(contestID),
                                                                                                       str(err)))
        traceback.print_stack()
        raise Exception("Contest {} not found".format(str(contestID)))


def updateCollectorParsingStatusForFinishedContest(databaseConnection, finishedContestID, isContestFinished):
    if isContestFinished:
        cursor = databaseConnection.cursor()
        cursor.execute("UPDATE CONTESTDATA SET FINALPARSINGDONE = 1 WHERE CONTESTID = " + str(finishedContestID))
        cursor.close()


def getCollectorImage(contestID, data, parentDirPath, isContestFinished, log):
    if isContestFinished:
        retry = 3
        while retry > 0:
            try:
                newpath = os.path.join(parentDirPath, contestID)
                if not os.path.exists(newpath):
                    os.mkdir(newpath)
                imagePath = os.path.join(newpath, data['EntryId'] + '.png')
                if not os.path.exists(imagePath):
                    response = requests.get(data['ImageURL'], stream=True)
                    with open(imagePath, 'wb') as outfile:
                        for chunk in response.iter_content(1024):
                            outfile.write(chunk)
                    del response

                data['ImageURL'] = os.path.relpath(imagePath, )
                break
            except Exception:
                data['ImageURL'] = str(None)
            retry -= 1


def createCollectorImageDir(parentDirPath, log):
    try:
        if not os.path.exists(parentDirPath):
            os.mkdir(parentDirPath)
    except Exception as err:
        log.error("Error while creating Image Dir {} => {}".format(parentDirPath, str(err)))
        traceback.print_stack()
        raise Exception("Error occurred while creating DIR {} => {}".format(parentDirPath, str(err)))
    return parentDirPath


def executeStatement(log, cursor, statement):
    retry = 1
    while retry <= 3:
        try:
            cursor.execute(statement)
            break
        except Exception as err:
            log.warning("Retrying cursor execute. Failed because of error => {}".format(err))
            time.sleep(50)
            retry += 1
    return retry <= 3


def getInvalidEntries(databaseConnection, contestID, log):
    try:
        cursor = databaseConnection.cursor()
        cursor.execute("SELECT ENTRYNUM FROM withdrawn WHERE CONTESTID = " + str(contestID))
        withdrawnEntries = set(cursor.fetchall())
        cursor.execute("SELECT ENTRYNUM FROM eliminated WHERE CONTESTID = " + str(contestID))
        eliminatedEntries = cursor.fetchall()
        result = set(withdrawnEntries)
        for member in eliminatedEntries:
            result.add(member)
        answer = set()
        for member in result:
            answer.add(str(member[0]))
        cursor.close()
        return answer
    except Exception as err:
        log.error(
            "Contest {} not found in the contestdata table. Please run ContestInfoCollector.py".format(str(contestID),
                                                                                                       str(err)))
        traceback.print_stack()
        raise Exception("Contest {} not found".format(str(contestID)))