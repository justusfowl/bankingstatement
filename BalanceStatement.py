import logging
import datetime
from datetime import timedelta
import getpass
import json
import pymysql

from fints.client import FinTS3PinTanClient
from pymongo import MongoClient
from pymongo import errors as PyMongoErrors
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc
from sqlalchemy import text
from sqlalchemy.sql import func
from sqlalchemy import create_engine, Column, Table, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Integer, SmallInteger, String, Date, DateTime, Float, Boolean, Text, LargeBinary)

DeclarativeBase = declarative_base()


def create_table(engine):
    DeclarativeBase.metadata.create_all(engine)


class Bank(DeclarativeBase):
    __tablename__ = 'tblbanks'
    bankId = Column(Integer(), primary_key=True)
    bankName = Column(String(100))
    bankUrl = Column(String(2000))

    def __repr__(self):
        return "<bank(bankName='%s', bankUrl='%s')>" % (
            self.bankName, self.bankUrl)


class Account(DeclarativeBase):
    __tablename__ = 'tblaccounts'
    accountNumber = Column(String(100), primary_key=True)
    accountBlz = Column(String(100), primary_key=True)
    accountLogin = Column(String(100))
    accountOwner = Column(String(100))
    bankId = Column(Integer(), ForeignKey("tblbanks.bankId"))

    def __repr__(self):
        return "<Account(accountNumber='%s', accountBlz='%s', accountOwner='%s')>" % (
            self.accountNumber, self.accountBlz, self.accountOwner)


class Account_Transaction(DeclarativeBase):
    __tablename__ = 'tblacctransactions'
    accountNumber = Column(Integer(), primary_key=True)
    accountBlz = Column(Integer(), primary_key=True)
    transactionAmt = Column(Float(17, 2), primary_key=True)
    transactionCur = Column(String(3))
    transactionType = Column(String(100))
    transactionTitle = Column(String(250), primary_key=True)
    transactionApplicantName = Column(String(250))
    transactionDate = Column(Date(), primary_key=True)
    transactionEntryDate = Column(Date())
    withdrawDate = Column(Date())
    transactionOwnerId = Column(String(100), ForeignKey("tblusers.userId"))

    def __repr__(self):
        return "<account_transaction(accountNumber='%s', accountBlz='%s', transactionAmt='%s', transactionDate='%s')>" % (
            self.accountNumber, self.accountBlz, self.transactionAmt, self.transactionDate)


class Run:
    def __init__(self, mysql_conn_string, mongo_conn_string, args):

        if args["accountLogin"] is None:
            login = args["accountNumber"]
        else:
            login = args["accountLogin"]

        if args["accountKey"] is None or args["accountKey"] == "":
            key = getpass.getpass('PIN for {}:'.format(login))
        else:
            key = args["accountKey"]

        fin_client = FinTS3PinTanClient(
            args["accountBlz"],
            login,
            key,
            args["bankUrl"]
        )

        self.accountOwnerId = args["accountOwner"]

        self.fin_client = fin_client
        self.lastWithdrawn = args["maxWithdrawDate"]

        self.accounts = fin_client.get_sepa_accounts()
        self.balances = []
        self.transactions = []
        self.fetched = {}

        self.errors = []
        self.duplicates = []
        self.success = {}

        # MySQL
        engine = create_engine(mysql_conn_string)
        self.Session = sessionmaker(bind=engine)

        # MongoDB
        self.client = MongoClient(mongo_conn_string)
        db = self.client.fin71
        self.tb_transactions = db.transactions

    def init_processing(self):
        session = self.Session()

        for acc in self.accounts:
            mongo_t_array, sql_t_array = self.process_transactions(acc)
            stored_mongo, stored_sql = self.store_transactions(mongo_t_array, sql_t_array)

            self.handle_success(acc, stored_sql)

            logging.info("For the account {}, {} transactions were retrieved, {} (mongo) and {} (sql) stored".format(
                acc.accountnumber, len(sql_t_array), stored_mongo, stored_sql))

        self.complete_processing()

    def complete_processing(self):

        self.client.close()
        logging.info("Processing complete")

        for acc in self.accounts:
            logging.info("There were {} transactions fetched for the account {}, {} added to the database".format(
                self.get_fetched(acc.accountnumber), acc.accountnumber, self.get_success(acc.accountnumber)))

    def process_transactions(self, account):

        balance = self.fin_client.get_balance(account)
        self.balances.append(balance)

        start, end = self.get_starting_date()

        statements = self.fin_client.get_statement(account, start, end)

        # how many transactions have been fetched for the time periode
        total_num = len(statements)
        self.handle_statements_fetched(account, total_num)

        mongo_dict = []
        sql_dict = []

        for s in statements:

            # Cater for faulty entry_date parsing:
            # If dates vary for more than 50 days, then parsing of entry_date is faulty
            # Replace with normal date

            if abs(s.data["date"] - s.data["entry_date"]) > datetime.timedelta(days=50):
                s.data["entry_date"] = s.data["date"]

            # if booking date is at least today or before
            if s.data["entry_date"] <= datetime.datetime.now():

                mongo_obj, sql_obj = self.make_transaction_dict(account, s.data)

                mongo_dict.append(mongo_obj)
                sql_dict.append(sql_obj)
            else:
                logging.info(
                    "The following transaction is to be expected in the future {} at {}".format(s.data["entry_date"],
                                                                                                s.data["purpose"]))

        return mongo_dict, sql_dict

    def handle_statements_fetched(self, account, total_num):
        self.fetched[account.accountnumber] = total_num

    def get_fetched(self, accountnumber):
        if accountnumber in self.fetched:
            return self.fetched[accountnumber]
        else:
            return 0

    def get_success(self, accountnumber):
        if accountnumber in self.success:
            return self.success[accountnumber]
        else:
            return 0

    def handle_success(self, account, num_items):

        self.success[account.accountnumber] = num_items

    def get_starting_date(self):

        if self.lastWithdrawn is None:
            start = datetime.datetime(2000, 1, 1)
        else:
            # if data has been withdrawn for this account, get one day previously and start from there
            start = self.lastWithdrawn + timedelta(-1)

        end = datetime.datetime.now()

        return start, end

    @staticmethod
    def sanitize_string(input_string):

        if input_string is None:
            return "#None#"
        if len(input_string) == 0:
            return "#None#"
        else:
            return input_string

    def get_transaction_item(self, account, data):

        return {
            "transactionOwnerId": self.accountOwnerId,
            "withdrawDate": datetime.datetime.now(),
            "accountNumber": account.accountnumber,
            "accountBlz": account.blz,
            "iban": account.iban,
            "bic": account.bic,
            "status": data.get("status", None),
            "funds_code": data.get("funds_code", None),
            "amount": data["amount"].amount.to_eng_string(),
            "id": data.get("id", None),
            "customer_reference": data.get("customer_reference", None),
            "bank_reference": data.get("bank_reference", None),
            "extra_details": data.get("extra_details", None),
            "currency": data.get("currency", None),
            "date": data["date"].isoformat(),
            "entry_date": data["entry_date"].isoformat(),
            "transaction_code": data.get("transaction_code", None),
            "posting_text": data.get("posting_text", ""),
            "prima_nota": data.get("prima_nota", None),
            "purpose": data.get("purpose", ""),
            "applicant_bin": data.get("applicant_bin", None),
            "applicant_iban": data.get("applicant_iban", None),
            "applicant_name": data.get("applicant_name", None),
            "return_debit_notes": data.get("return_debit_notes", None),
            "recipient_name": data.get("recipient_name", None),
            "additional_purpose": data.get("additional_purpose", None),
            "gvc_applicant_iban": data.get("gvc_applicant_iban", None),
            "gvc_applicant_bin": data.get("gvc_applicant_bin", None),
            "end_to_end_reference": data.get("end_to_end_reference", None),
            "additional_position_reference": data.get("additional_position_reference", None),
            "applicant_creditor_id": data.get("applicant_creditor_id", None),
            "purpose_code": data.get("purpose_code", None),
            "additional_position_date": data.get("additional_position_date", None),
            "deviate_applicant": data.get("deviate_applicant", None),
            "deviate_recipient": data.get("deviate_recipient", None),
            "FRST_ONE_OFF_RECC": data.get("FRST_ONE_OFF_RECC", None),
            "old_SEPA_CI": data.get("old_SEPA_CI", None),
            "old_SEPA_additional_position_reference": data.get("old_SEPA_additional_position_reference", None),
            "settlement_tag": data.get("settlement_tag", None),
            "debitor_identifier": data.get("debitor_identifier", None),
            "compensation_amount": data.get("compensation_amount", None),
            "original_amount": data.get("original_amount", None)
        }

    def make_transaction_dict(self, account, transaction_data):

        mongo_t_itm = self.get_transaction_item(account, transaction_data)

        sql_acc_trans_item = Account_Transaction(
            accountNumber=mongo_t_itm["accountNumber"],
            accountBlz=mongo_t_itm["accountBlz"],
            transactionAmt=mongo_t_itm["amount"],
            transactionCur=mongo_t_itm["currency"],
            transactionType=mongo_t_itm["posting_text"],
            transactionTitle=self.sanitize_string(mongo_t_itm["id"]) + "/" + self.sanitize_string(
                mongo_t_itm["purpose"]),
            transactionApplicantName=mongo_t_itm["applicant_name"],
            transactionDate=mongo_t_itm["date"],
            transactionEntryDate=mongo_t_itm["entry_date"],
            withdrawDate=mongo_t_itm["withdrawDate"],
            transactionOwnerId=mongo_t_itm["transactionOwnerId"]
        )

        return mongo_t_itm, sql_acc_trans_item

    def store_transactions(self, mongo_t_array, sql_t_array):

        def _store_sql_transaction(self, sql_item):
            try:

                session = self.Session()

                session.add(sql_item)

                session.commit()

                return True

            except exc.SQLAlchemyError as e:

                err_msg = e.args[0]
                session.rollback()
                logging.error(err_msg)

                if "1062," in err_msg:
                    logging.warning("error MySQL, transaction with id {} already exists".format(sql_item))
                else:
                    self.errors.append(sql_item)
                    logging.warning("unknown error adding transaction to mysql {}".format(sql_item))

                return False

            except Exception as e:
                logging.warning("unknown error {}".format(e))
                return False

        def _store_mongo_transaction(self, mongo_item):

            try:

                self.tb_transactions.insert_one(mongo_item)

                return True

            except PyMongoErrors.DuplicateKeyError as e:
                logging.warning("error MongoDB, transaction with id {} already exists".format(e))
                return False
            except Exception as e:
                logging.warning("unknown error {}".format(e))
                return False

        stored_mongo = 0
        stored_sql = 0

        for m in mongo_t_array:
            res = _store_mongo_transaction(self, m)
            if res:
                stored_mongo += 1

        for t in sql_t_array:
            res = _store_sql_transaction(self, t)
            if res:
                stored_sql += 1

        return stored_mongo, stored_sql