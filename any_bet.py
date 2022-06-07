import mysql.connector
import requests
import time
import datetime
import json

""" Auto-scraping website script.

    This script finds information about soccer games on a particular site, 
    reads detailed information and saves it in an ordered form to the database. Every 15 minutes.

    P.S. $%#bet.co.za
    You can put another link, but it won't work.
    The script is configured for a specific site, taking into account its features..
"""


def pars_bet():
    conn = mysql.connector.connect(host='$$$$$$$',
                                   database='footballdata',
                                   user='$$$$$$$',
                                   password='$$$',
                                   port='$$$$$$$')
    cursor = conn.cursor()
    print('Connection for MySQL')
    date = datetime.datetime.today()
    d = date.day
    m = date.month
    y = date.year
    t = (((str(date.time())).split('.')[0]).split(':'))
    if d == '30' or d == '31' or (d == '29' and m == '2'):
        url = 'https://$%#.org/offering/v2018/siwc/listView/all/all/all/all/starting-within.json' \
              '?lang=en_ZA&market=ZA&client_id=2&channel_id=1&ncid=1636885523302&useCombined=true&' \
              'from={}{}{}T{}{}{}%2B0300&to={}{}{}T24{}{}%2B0300'.format(y, m, d, t[0], t[1], t[2], y, m, d, t[1], t[2])
    else:
        url = 'https://$%#.org/offering/v2018/siwc/listView/all/all/all/all/starting-within.json' \
              '?lang=en_ZA&market=ZA&client_id=2&channel_id=1&ncid=1636885523302&useCombined=true&' \
              'from={}{}{}T{}{}{}%2B0300&to={}{}{}T{}{}{}%2B0300'.format(y, m, d, t[0], t[1], t[2], y, m,
                                                                         str(int(d)+2), t[0], t[1], t[2])
    req = requests.get(url)
    src = req.text
    with open("$%#bet.json", "w", encoding="utf-16", errors='ignore') as file:
        file.write(src)

    cursor.execute('DELETE FROM $%#bet;', )
    conn.commit()

    with open("$%#bet.json", encoding="utf-16", errors='ignore') as file:
        src = file.read()
    json_string = src
    parsed_string = json.loads(json_string)
    for item in parsed_string['events']:
        if (item['event']['path'][0]['englishName']).lower() == 'football':
            try:
                sport = item['event']['path'][0]['englishName']
                country = item['event']['path'][1]['englishName']
                tournament = item['event']['path'][2]['englishName']
                hometeam = item['event']['homeName']
                awayteam = item['event']['awayName']
                date_date = item['event']['start']
                dt = date_date.split('T')[0]
                date_time = (date_date.split('T')[1]).split(':')[0] + ':' + (date_date.split('T')[1]).split(':')[1]
                h = item['betOffers'][0]['outcomes'][0]['oddsFractional']
                d = item['betOffers'][0]['outcomes'][1]['oddsFractional']
                a = item['betOffers'][0]['outcomes'][2]['oddsFractional']
                homewin = round([int(i) for i in (h.split('/'))][0] / [int(i) for i in (h.split('/'))][1], 2)
                draw = round([int(i) for i in (d.split('/'))][0] / [int(i) for i in (d.split('/'))][1], 2)
                awaywin = round([int(i) for i in (a.split('/'))][0] / [int(i) for i in (a.split('/'))][1], 2)
            except Exception as json_error:
                print(json_error)
                continue
            try:
                cursor.execute("INSERT INTO $%#bet(Sport, Country, Tournament, HomeTeam, AwayTeam, "
                               "Date, Time, HomeWin, Draw, AwayWin) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               (sport, country, tournament, hometeam, awayteam,
                                dt, date_time, homewin, draw, awaywin))
                conn.commit()
                print('+', sport, country, tournament, hometeam, awayteam,
                      dt, date_time, homewin, draw, awaywin)
            except Exception as connection_error:
                print(connection_error)
                continue
        else:
            continue
    else:
        print('Connection closed')
        print('time sleep = 900')
        conn.close()


if __name__ == '__main__':
    while True:
        try:
            pars_bet()
            time.sleep(900)
        except Exception as e:
            print(e)
            time.sleep(600)
