import sys
import argparse
import logging
import sys
import os

from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy import create_engine

from dotenv import load_dotenv
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

from BalanceStatement import Run

parser = argparse.ArgumentParser(description='kDatacenter Kontoauszug')

parser.add_argument("-u", "--user_ids", nargs='+',
                    help="userIds to be used for withdrawing statements", metavar="STRINGS")

logging.basicConfig(level=logging.INFO)

def main(args=sys.argv[1:]):

    def _get_accounts(user_id):
        engine = create_engine(mysql_conn_string)
        Session = sessionmaker(bind=engine)
        sess = Session()

        sql_cmd = text('''SELECT acc.*, b.*, d.maxWithdrawDate FROM 
        fin71.tblaccounts as acc 
        left join fin71.tblbanks as b on acc.bankId = b.bankId
        left join (
            select 
                accountNumber, 
                accountBlz, 
                max(withdrawDate) as maxWithdrawDate
            FROM fin71.tblacctransactions
            GROUP BY accountNumber, accountBlz) as d on (acc.accountNumber = d.accountNumber And acc.accountBlz = d.accountBlz)
            where acc.accountOwner = :accountOwner''')

        options = {
            "accountOwner": user_id
        }

        accounts   = []

        for acc in sess.execute(sql_cmd, options):
            accounts.append(acc)

        return accounts

    args = parser.parse_args(args)

    mysql_conn_string = "{drivername}://{user}:{passwd}@{host}:{port}/{db_name}?charset=utf8".format(
        drivername="mysql+pymysql",
        user=os.environ.get("MYSQL_USER"),
        passwd=os.environ.get("MYSQL_PASS"),
        host=os.environ.get("MYSQL_HOST"),
        port=os.environ.get("MYSQL_PORT"),
        db_name=os.environ.get("MYSQL_DB")
    )

    mongo_conn_string = "mongodb://{user}:{passwd}@{host}:{port}/{db_name}".format(
        user=os.environ.get("MONGO_USER"),
        passwd=os.environ.get("MONGO_PASS"),
        host=os.environ.get("MONGO_HOST"),
        port=os.environ.get("MONGO_PORT"),
        db_name=os.environ.get("MONGO_DB")
    )

    user_ids = args.user_ids

    for u in user_ids:

        accounts = _get_accounts(u)

        print(accounts)

        for acc in accounts:
            r = Run(mysql_conn_string, mongo_conn_string, acc)
            r.init_processing()

main(sys.argv[1:])