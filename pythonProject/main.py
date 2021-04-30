import csv
import openpyxl
import pandas as pd
import psycopg2
import random

try:
    conn = psycopg2.connect(
        host="localhost",
        database="G2DB",
        user="postgres",
        password="Dec$15941")
except:
    print('Unable to connect to DB...')


def pgQuery(query):
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        rows = cur.fetchall()
        # cur.close()
        # conn.close()
        for row in rows:
            return row[0]
    except Exception as e:
        raise e


def csvReader():
    with open('IT - Underwriting G2 URLs.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = [item.lower() for item in next(reader)]
        print(header)
        # Merchant Table
        corp_name, dba_name, billing_descriptor, mid, group_id = header.index('corporate name'), \
                                                                 header.index('dba name'), \
                                                                 header.index('billing descriptor'), \
                                                                 header.index('mid'), \
                                                                 1
        # Urls Table
        # Merchant ID will be generated
        # Commission ID will be generated
        merchant_id, commission_id, url, mcc, username, password, tc, pp, refund_policy, code_182257, ssl_https = 0, \
                                                                                                                  0, \
                                                                                                                  header.index(
                                                                                                                      'website url'), \
                                                                                                                  header.index(
                                                                                                                      'mcc'), \
                                                                                                                  header.index(
                                                                                                                      'username'), \
                                                                                                                  header.index(
                                                                                                                      'password'), \
                                                                                                                  header.index(
                                                                                                                      "t & c's"), \
                                                                                                                  header.index(
                                                                                                                      'privacy policy'), \
                                                                                                                  header.index(
                                                                                                                      'refund policy'), \
                                                                                                                  header.index(
                                                                                                                      '18 2257'), \
                                                                                                                  header.index(
                                                                                                                      'https')
        # Commissions Table
        agent1, agent2, agent3, agent4 = header.index('agent #1'), header.index('agent #2'), header.index(
            'agent#3'), header.index('agent#4')
        # pgQuery(
        #     "INSERT INTO public.urls(merchant_id,commission_id, url, mcc, username, password, tc,pp,refund_policy,code_182257,ssl_https)"
        #     " VALUES ({},{},'{}','{}','{}','{}',{},{},{},{},{}) RETURNING id;".format(1, 1,
        #                                                                        'test.com', '8080', 'Test',
        #                                                                        'Test', True, True,
        #                                                                        True, True,
        #                                                                        True))

        for row in reader:
            if len(row) == 22:
                rand_group_id = random.choice(range(7, 11))
                query_merch_id = pgQuery(
                    """INSERT INTO public.merchants(group_id, corp_name, dba_name, mid) VALUES (%s,'%s','%s','%s') RETURNING id;""" % (
                    rand_group_id, row[corp_name].replace("'","''"), row[dba_name], row[mid]))

                # query_commission_id = pgQuery("""INSERT INTO public.commissions(agent_1,agent_2,agent_3,agent_4) VALUES('''%s''','''%s''','''%s''','''%s''') RETURNING id;""" % (row[agent1],row[agent2],row[agent3],row[agent4]))

                pgQuery("""INSERT INTO public.urls(merchant_id, url, mcc, username, password, tc,pp,refund_policy,code_182257,ssl_https) 
                VALUES (%s,'%s','%s','%s','%s',%s,%s,%s,%s,%s) RETURNING id;""" % (
                    query_merch_id,
                    row[url],
                    row[mcc], row[username],
                    row[password],
                    True if row[tc] == 'Y' else False,
                    True if row[pp] == 'Y' else False,
                    True if row[refund_policy] == 'Y' else False,
                    True if row[code_182257] == 'Y' else False,
                    True if row[ssl_https] == 'Y' else False))


if __name__ == '__main__':
    csvReader()
