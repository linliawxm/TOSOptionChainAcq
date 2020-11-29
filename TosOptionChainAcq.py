import requests
import json
from stocksymbols  import stocksymbols
import pyodbc
import time
from datetime import datetime as dt
import threading, queue
from serverinfo  import serverInfo,clientId
import logging

# w_str = ['Start', 'UpRate', 'UpFailSymbol', 'DownRate', 'DownFailSymbol', 'Finish']
# value = ['2020-10-21 11:00','100.00%',['AAPL','SPY'],'100.00%',['AAPL','SPY'],'2020-10-22 10:00']
# with open('log.txt','a') as f:
#     itemName = " ".join([str(elem) for elem in value])
#     f.write(itemName)

#Record debug info for diagnostic
logging.basicConfig(filename=r'C:/Users/rsurveillance/Desktop/python_OI/debug.log', format='%(asctime)s %(message)s', level=logging.ERROR)
#Record all detail activities
now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
recording = [now]
logging.info("Started")

down_num_sym = 0
download_done = False
total_sym_num = len(stocksymbols)
data_q = queue.Queue(total_sym_num)

#define our endpint: Get the Option chain data
endpoint = r'https://api.tdameritrade.com/v1/marketdata/chains'

def UploadToSqlThread():
    global down_num_sym
    global download_done
    global recording
    server = serverInfo['SERVER'] 
    database = serverInfo['DATABASE'] 
    username = serverInfo['USERNAME'] 
    password = serverInfo['PASSWORD'] 
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        num_sym_up = 0
        upload_done = False
        while upload_done == False:
            #Read data from queue
            data = data_q.get()
            isSuccess = False
            tryCnt = 0
            while tryCnt < 3 and isSuccess == False:
                tryCnt = tryCnt + 1
                if num_sym_up == 0:
                    up_start_time = time.time()
                UploadDateTime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                failedSym=[]
                #Upload data for OptionChainHeader
                try:
                    headerList = [data['symbol'],data['Trading_Day'],data['Trading_DateTime'],data['underlyingPrice'],UploadDateTime ]

                    var_string = ', '.join('?' * len(headerList))
                    query_string = 'INSERT INTO OptionChainHeader VALUES (%s);' % var_string
                    cursor.execute(query_string, headerList)
                except pyodbc.DataError as errd:
                    conn.rollback()    # probably unnecessary
                    header_err = '{0}:{1} header record insert failed: {2}'.format(UploadDateTime, data['symbol'], errd)
                    logging.error(header_err)
                    #print(header_err)
                    #failedSym.append(data['symbol'])
                else:
                    detail_fail = True
                    headerRecId = cursor.execute('SELECT @@IDENTITY AS id;').fetchone()[0]
                    if headerRecId:
                        #conn.commit()  #Commit until details finished
                        #Insert PUT contract
                        cont_num = 0
                        all_rows = []
                        for contDays in data['putExpDateMap'].values():
                            for contPrices in contDays.values():
                                #delete unused data
                                del contPrices[0]['bidAskSize']
                                del contPrices[0]['tradeTimeInLong'] 
                                del contPrices[0]['quoteTimeInLong'] 
                                del contPrices[0]['optionDeliverablesList']
                                del contPrices[0]['isIndexOption']
                                #update date format from seconds to date
                                l_time = contPrices[0]['expirationDate']/1000
                                contPrices[0]['expirationDate'] = dt.fromtimestamp(l_time).strftime("%Y-%m-%d")
                                l_time = contPrices[0]['lastTradingDay']/1000
                                contPrices[0]['lastTradingDay'] = dt.fromtimestamp(l_time).strftime("%Y-%m-%d")
                                #contPrices[0]['HeaderRecID'] = int(headerRecId)
                                #Update Boolean True&False to 1&0
                                contPrices[0]['inTheMoney'] = int(contPrices[0]['inTheMoney'])
                                contPrices[0]['mini'] = int(contPrices[0]['mini'])
                                contPrices[0]['nonStandard'] = int(contPrices[0]['nonStandard'])
                                contPrices[0]['tradeDate'] = data['Trading_Day']

                                if contPrices[0]['putCall'] == 'PUT':
                                    contPrices[0]['putCall'] = 'P'
                                else:
                                    contPrices[0]['putCall'] = 'C'

                                values = list(contPrices[0].values())
                                values.insert(0,int(headerRecId))
                                if 'NaN' not in values:
                                    #placeholders = ', '.join(['?']* len(values)) 
                                    #dict_values = ', '.join("'" + str(x).replace('/', '_') + "'" for x in values)
                                    all_rows.append(values)
                                    
                                    cont_num = cont_num + 1
                                    #print('{0}:{1} Contract:{2} uploaded'.format(num_sym_up,data['symbol'],cont_num))
                        var_string = ', '.join('?' * len(values))
                        query_string = "INSERT INTO OptionChainDetails VALUES ( %s );" % (var_string)
                        if all_rows:
                            try:
                                cursor.executemany(query_string,all_rows)
                            except:
                                conn.rollback()
                                exe_many_err = '{0} details executemany failed'.format(data['symbol'])
                                logging.error(exe_many_err)
                                #print(exe_many_err)
                                #failedSym.append(data['symbol'])
                            else:
                                detail_fail = False
                        else:
                            conn.rollback()
                            empty_err = '{0} details uploading failed: Empty record'.format(data['symbol'])
                            logging.error(empty_err)
                            #print(empty_err)
                            #failedSym.append(data['symbol'])
                    else:
                        conn.rollback()
                        recId_err = '{0} header RecId retrieve failed'.format(data['symbol'])
                        logging.error(recId_err)
                        #print(recId_err)
                        #failedSym.append(data['symbol'])
                    if not detail_fail:
                        conn.commit()
                        isSuccess = True
                        num_sym_up = num_sym_up + 1
                        print('{0}:{1} uploaded'.format(num_sym_up,data['symbol']))
                        if download_done == True and num_sym_up == down_num_sym:
                             upload_done = True
                if isSuccess == False:
                    #if failed, maybe caused by connection error, reconnect it
                    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
            if isSuccess == False:
                #3 times trial still failed, log the symbol
                failedSym.append(data['symbol'])   
                           
        conn.close()
    except pyodbc.Error as err:
        logging.error(err)
    up_end_time = time.time()
    up_detal = int(up_end_time - up_start_time)
    up_fail_num = total_sym_num - num_sym_up
    up_finished_str = '{0} symbols uploaded, {1} failed:{2} seconds elapsed'.format(num_sym_up, up_fail_num, up_detal)
    print(up_finished_str)
    logging.info(up_finished_str)
    upPercentage = "{:.2%}".format(num_sym_up/total_sym_num)
    recording.append(upPercentage)
    recording.append(failedSym)

                
th = threading.Thread(target=UploadToSqlThread)
th.start()


start_time = time.time()
down_num_sym = 0
down_fail_num = 0
#Downloading process
for symbol in stocksymbols:
    isSuccess = False
    tryCnt = 0
    while tryCnt < 3 and isSuccess == False:
        tryCnt = tryCnt + 1
        #time.sleep(0.6)
        downFailSym = []
        #define our payload
        payload = {'apikey':clientId,
                    'symbol':symbol}
        try:
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            #make a request
            content = requests.get(url=endpoint,params=payload)
            content.raise_for_status()
        except requests.exceptions.HTTPError as errh:
            logging.error('Http error - ' + str(errh))
            #downFailSym.append(symbol)
        except requests.exceions.ConnectionError as errc:
            logging.error('Error Connecptting:' + str(errc))
            #downFailSym.append(symbol)
        except requests.exceptions.Timeout as errt:
            logging.error('Timeout Error:' + str(errt))
            #downFailSym.append(symbol)
        except requests.exceptions.RequestException as err:
            logging.error('Download request failed:' + str(err))
            #downFailSym.append(symbol)
        else:#Downloading successfully
            isSuccess = True
            #convert it to disctionary
            data = content.json()
            #Add addtional two column data
            data['Trading_DateTime'] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            data['Trading_Day'] = time.strftime("%Y-%m-%d", time.localtime())
            #Push the data to queue which will be processed by Uploading thread
            data_q.put(data)

            down_num_sym = down_num_sym + 1
            now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

            print('{0}:{1} downloaded'.format(down_num_sym,symbol))
            #Save data to file
            #filename = symbol + '.json'
            #with open(filename, 'w+') as f:
            #    json.dump(data,f,indent=3)
    if isSuccess == False:
        downFailSym.append(symbol)
download_done = True
end_time = time.time()
detal = int(end_time - start_time)
failed_num = total_sym_num - down_num_sym
down_finish_str = '{0} symbols downloaded, {1} failed:{2} seconds elapsed'.format(down_num_sym, failed_num, detal)
downPercent="{:.2%}".format(down_num_sym/total_sym_num)
recording.append(downPercent)
recording.append(downFailSym)
#wait until uploading threading finished
th.join()

recording.append(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
with open(r'C:/Users/rsurveillance/Desktop/python_OI/log.txt','a') as f:
    recordValue = " ".join([str(elem) for elem in recording])
    f.write(recordValue)
    f.write('\n')

print(down_finish_str)
print('Downloading and uploading are finished')