#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests
import sys
import time
import optparse
import mariadb
try:
        import json as simplejson
except ImportError:
        import simplejson

mainUrl = 'https://api.schiphol.nl/public-flights/'

def getPublicFlights(options,page):

    url = mainUrl + 'flights'
    headers = {
        'accept': 'application/json',
        'resourceversion': 'v4',
        'app_id': options.app_id,
        'app_key': options.app_key
    }
    paramString = {'page': page}

    try:
        response = requests.request('GET', url, headers=headers, params=paramString)
    except requests.exceptions.ConnectionError as error:
        print(error)
        sys.exit()

    if response.status_code == 200:
        dataDict = response.json()
        dataList = dataDict["flights"]
        dataLen = len(dataList)

        if dataLen == 0:
            return None

        ''''
        for flight in Listflights:
            print('Found flight with name: {} scheduled on: {} at {}'.format(
            flight['flightName'],
            flight['scheduleDate'],
            flight['scheduleTime']))
            #InsertFlight(flight)
        else:
            print('Error: Wrong Http response code: {} {}'.format(response.status_code, response.text))
        '''
    else:
        return None
    return dataList

def getPublicFlightbyId(options,id):
    url = mainUrl + 'flights/' + id
    querystring = {'app_id': options.app_id, 'app_key': options.app_key}
    headers = {'resourceversion': 'v3'}

    try:
        response = requests.request('GET', url, headers=headers,params=querystring)
    except requests.exceptions.ConnectionError as e:
        print (e)
        sys.exit()

    if response.status_code == 200:
        flightdata = response.json()
        print (u'Found id {} flight with name: {} scheduled on: {} at {}'.format(flightdata['id'],
        flightdata['flightName'], flightdata['scheduleDate'], flightdata['scheduleTime']))
    else:
        nextGet = None
        print ('''Oops something went wrong
Http response code: {}
{}'''.format(response.status_code,response.text))

def getPublicDestinations(options,page):
    url = mainUrl + 'destinations'
    headers = {
        'accept': 'application/json',
        'resourceversion': 'v4',
        'app_id': options.app_id,
        'app_key': options.app_key
    }
    paramString = {'page': page}

    try:
        response = requests.request('GET', url, headers=headers, params=paramString)
    except requests.exceptions.ConnectionError as error:
        print(error)
        sys.exit()

    if response.status_code == 200:
        dataDict = response.json()
        dataList = dataDict["destinations"]
        dataLen = len(dataList)
        if dataLen == 0:
            return None

    return dataList

def getAirCraftType(options,page):
    url = mainUrl + 'aircrafttypes'

    headers = {
        'accept': 'application/json',
        'resourceversion': 'v4',
        'app_id': options.app_id,
        'app_key': options.app_key
    }
    paramString = {'page': page}

    try:
        response = requests.request('GET', url, headers=headers, params=paramString)
    except requests.exceptions.ConnectionError as error:
        print(error)
        sys.exit()

    if response.status_code == 200:
        dataDict = response.json()
        dataList = dataDict["aircraftTypes"]
        dataLen = len(dataList)

        if dataLen == 0:
            return None

    return dataList

def getAirLine(options,page):
    url = mainUrl + 'airlines'

    headers = {
        'accept': 'application/json',
        'resourceversion': 'v4',
        'app_id': options.app_id,
        'app_key': options.app_key
    }
    paramString = {'page': page}

    try:
        response = requests.request('GET', url, headers=headers, params=paramString)
    except requests.exceptions.ConnectionError as error:
        print(error)
        sys.exit()

    if response.status_code == 200:
        dataDict = response.json()
        dataList = dataDict["airlines"]
        dataLen = len(dataList)

        if dataLen == 0:
            return None

    return dataList

#--------------------------------------------------------------------------------------------------------------------------
def printprogres(index,amount):

    if index % amount == 0:
        sys.stdout.write('.')
        sys.stdout.flush()
#--------------------------------------------------------------------------------------------------------------------------
def insertPublicFlights(cursor,flightList):
    print('Inserting into DB')
    for i in range(len(flightList)):
        insertFlight(cursor, flightList[i])
        printprogres(i, 50)
    print('\nProcessed :', i)
    return

def insertFlight(cursor,flight):
    try:
        cursor.execute(
            "INSERT INTO flights (id, flightName, scheduleDate, flightDirection, flightNumber, prefixIATA, prefixICAO, scheduleTime, serviceType, mainFlight, estimatedLandingTime, actualLandingTime, publicEstimatedOffBlockTime, actualOffBlockTime, terminal, gate, expectedTimeOnBelt, aircraftRegistration, airlineCode, expectedTimeGateOpen, expectedTimeBoarding, expectedTimeGateClosing, schemaVersion) VALUES (%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (flight['id'],
             flight['flightName'],
             flight['scheduleDate'],
             flight['flightDirection'],
             flight['flightNumber'],
             flight['prefixIATA'],
             flight['prefixICAO'],
             flight['scheduleTime'],
             flight['serviceType'],
             flight['mainFlight'],
             flight['estimatedLandingTime'],
             flight['actualLandingTime'],
             flight['publicEstimatedOffBlockTime'],
             flight['actualOffBlockTime'],
             flight['terminal'],
             flight['gate'],
             flight['expectedTimeOnBelt'],
             flight['aircraftRegistration'],
             flight['airlineCode'],
             flight['expectedTimeGateOpen'],
             flight['expectedTimeBoarding'],
             flight['expectedTimeGateClosing'],
             flight['schemaVersion']))

        #cursor.execute(
        #   "INSERT INTO flights (id, flightName) VALUES (%s,%s)",
        #    (flight['id'],flight['flightName']))

    except mariadb.Error as error:
        print("Error: {}".format(error))

    conn.commit()
    #print("The last inserted id was: ", cursor.lastrowid)

    #if cursor.lastrowid = None:
    #    print('stop')

def insertAirCraftTypes(cursor,aircraftList):
    print('Inserting into DB')
    for i in range(len(aircraftList)):
        insertAirCraftType(cursor, aircraftList[i])
        printprogres(i, 50)
    print('\nProcessed :', i)
    return

def insertAirCraftType(cursor,airCraftType):
    try:
        cursor.execute(
            "INSERT INTO aircraft_types (longDescription, shortDescription, iatamain, iatasub) VALUES (%s,%s,%s,%s)",
            (airCraftType['longDescription'], airCraftType['shortDescription'], airCraftType['iataMain'], airCraftType['iataSub']))

    except mariadb.Error as error:
        print("Error: {}".format(error))

    conn.commit()
    #print("The last inserted id was: ", cursor.lastrowid)

def insertPublicDestinations(cursor,destinationList):
    print('Inserting into DB')
    for i in range(len(destinationList)):
        insertDestination(cursor, destinationList[i])
        printprogres(i, 50)
    print('\nProcessed :', i)
    return

def insertDestination(cursor,destination):
    try:
        cursor.execute(
            "INSERT INTO destinations (iata, country, city) VALUES (%s,%s,%s)",
            (destination['iata'], destination['country'], destination['city']))

    except mariadb.Error as error:
        print("Error: {}".format(error))
    conn.commit()

def insertAirlines(cursor,aList):
    print('Inserting into DB')
    for i in range(len(aList)):
        insertAirline(cursor, aList[i])
        printprogres(i, 50)
    print('\nProcessed :', i)
    return

def insertAirline(cursor,airLine):
    try:
        cursor.execute(
            "INSERT INTO airlines (iata, icoa, publicName, nvls) VALUES (%s,%s,%s,%s)",
            (airLine['iata'], airLine['icao'], airLine['publicName'], airLine['nvls']))

    except mariadb.Error as error:
        print("Error: {}".format(error))
    conn.commit()

def removeNoneEntries(grid):
    newGrid = []

    for i in range(0, len(grid)):
        newLine = grid[i]
        if '' not in newLine and 'None' not in newLine and None not in newLine:
            newGrid.append(newLine)

    return newGrid

def addRowsToList(List, addList):
    lenAddList = len(addList)

    for i in range(lenAddList):
        List.append(addList[i])

    return List

def getAllPublicFlights(options):
        timePause = 60 / float(200) + 1
        maxRequests = 6000
        maxQueries = int(maxRequests / 20)

        aList = []

        print('Getting the Flights ...')
        for page in range(maxQueries):
            addListRaw = getPublicFlights(options, page)

            if isinstance(addListRaw, list):
                addList = removeNoneEntries(addListRaw)
                aList = addRowsToList(aList, addList)
                # time.sleep(timePause)
            else:
                break

            printprogres(len(aList), 50)

        print('\nFlights scheduled: {}'.format(len(aList)))
        return aList

def getAllPublicDestinations(options):
    timePause = 60 / float(200) + 1
    maxRequests = 6000
    maxQueries = int(maxRequests / 20)

    aList = []

    print('Getting the Destinations ...')
    for page in range(maxQueries):
        addListRaw = getPublicDestinations(options, page)

        if isinstance(addListRaw, list):
            addList = removeNoneEntries(addListRaw)
            aList = addRowsToList(aList, addList)
            # time.sleep(timePause)
        else:
            break

        printprogres(len(aList), 250)

    print('\nDestinations: {}'.format(len(aList)))
    return aList

def getAirCraftTypes(options):
        timePause = 60 / float(200) + 1
        maxRequests = 6000
        maxQueries = int(maxRequests / 20)

        aList = []

        print('Getting the AirCraftTypes ...')
        for page in range(maxQueries):
            addListRaw = getAirCraftType(options, page)

            if isinstance(addListRaw, list):
                addList = removeNoneEntries(addListRaw)
                aList = addRowsToList(aList, addList)
                # time.sleep(timePause)
            else:
                break

            printprogres(len(aList), 20)

        print('\nAirCraftTypes: {}'.format(len(aList)))
        return aList


def getAirLines(options):
    timePause = 60 / float(200) + 1
    maxRequests = 6000
    maxQueries = int(maxRequests / 20)

    aList = []

    print('Getting the AirLines ...')
    for page in range(maxQueries):
        addListRaw = getAirLine(options, page)

        if isinstance(addListRaw, list):
            addList = removeNoneEntries(addListRaw)
            aList = addRowsToList(aList, addList)
            # time.sleep(timePause)
        else:
            break

        printprogres(len(aList), 20)

    print('\nAirlines: {}'.format(len(aList)))
    return aList
#     -------------------- main --------------------------
if __name__ == '__main__':
    parser = optparse.OptionParser()
    parser.add_option('-i', '--app_id', dest='app_id',
                      help='App id used to call the API')
    parser.add_option('-k', '--app_key', dest='app_key',
                      help='App key used to call the API')

    (options, args) = parser.parse_args()
    if options.app_id is None:
        parser.error('Please provide an app id (-i, --app_id)')

    if options.app_key is None:
        parser.error('Please provide an app key (-key, --app_key)')

    try:
        conn = mariadb.connect(host="*.*.*.*",
                               port=3306,
                               user='*',
                               password='****',
                               database='schiphol')
    except mariadb.Error as e:
        print(f"Error connecting to mariaDB Platform {e}")

    # get Cursor
    cursor = conn.cursor()

    flights = getAllPublicFlights(options)
    insertPublicFlights(cursor, flights)
    #getPublicFlightbyId(options,'125648415216510969')

    #destinations = getAllPublicDestinations(options)
    #insertPublicDestinations(cursor, destinations)

    #airCraftTypes = getAirCraftTypes(options)
    #insertAirCraftTypes(cursor, airCraftTypes)

    #airLines = getAirLines(options)
    #insertAirlines(cursor, airLines)
