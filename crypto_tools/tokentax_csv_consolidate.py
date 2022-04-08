#!/usr/bin/env python3
"""Contains the main package functionality."""

from __future__ import annotations

import argparse
import csv
import enum
import locale
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List

locale.setlocale(locale.LC_ALL, "")
currency_symbol = str(locale.localeconv()["currency_symbol"])

package_logger = logging.getLogger(__package__)
logger = logging.getLogger(__name__)


class TokenTaxTransactionType(enum.Enum):
    """Types of transactions accepted by TokenTax."""

    TRADE = "Trade"
    INCOME = "Income"
    AIRDROP = "Airdrop"
    BORROW = "Borrow"
    DEPOSIT = "Deposit"
    FORK = "Fork"
    GIFT = "Gift"
    LOST = "Lost"
    MIGRATION = "Migration"
    MINING = "Mining"
    REPAY = "Repay"
    SPEND = "Spend"
    STAKING = "Staking"
    STOLEN = "Stolen"
    WITHDRAWAL = "Withdrawal"


@dataclass
class TokenTaxTransaction:
    """Holds a transaction entry in CSV."""

    transaction_type: TokenTaxTransactionType
    buy_amount: float
    buy_currency: str
    sell_amount: float
    sell_currency: str
    fee_amount: float
    fee_currency: str
    exchange: str
    exchange_id: str
    group: str
    import_name: str
    comment: str
    date: datetime
    usd_equivalent: float
    updated_at: datetime

    @staticmethod
    def create_from_transaction_dict(transaction_dict: Dict[str, str]) -> TokenTaxTransaction:
        """Initialize transaction from csv DictReader row."""
        return TokenTaxTransaction(
            TokenTaxTransactionType(transaction_dict["Type"]),
            float(transaction_dict["BuyAmount"]),
            transaction_dict["BuyCurrency"],
            float(transaction_dict["SellAmount"]),
            transaction_dict["SellCurrency"],
            float(transaction_dict["FeeAmount"]),
            transaction_dict["FeeCurrency"],
            transaction_dict["Exchange"],
            transaction_dict["ExchangeId"],
            transaction_dict["Group"],
            transaction_dict["Import"],
            transaction_dict["Comment"],
            datetime.strptime(transaction_dict["Date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
            locale.atof(transaction_dict["USDEquivalent"].strip(currency_symbol)),
            datetime.strptime(transaction_dict["UpdatedAt"], "%Y-%m-%dT%H:%M:%S.%fZ"),
        )


# def get_transaction_type_from_transaction(transaction: str)


def main() -> None:
    """Do the main thing."""
    # Get arguments
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show DEBUG level logging messages")
    parser.add_argument("input_tokentax_csv_file", type=str, help="Path to TokenTax CSV file to manipulate")
    args = parser.parse_args()

    # Use root python logger to set loglevel
    if args.verbose is True:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    input_tokentax_csv_file_path = Path(args.input_tokentax_csv_file)
    if not input_tokentax_csv_file_path.exists():
        logger.error("Input TokenTax CSV file does not exist at location: %s", input_tokentax_csv_file_path)
        sys.exit(1)

    transaction_list: List[TokenTaxTransaction] = []
    with input_tokentax_csv_file_path.open("r") as input_tokentax_csv_file:
        dialect = csv.Sniffer().sniff(input_tokentax_csv_file.read(1024))
        input_tokentax_csv_file.seek(0)
        tokentax_transaction_dictreader = csv.DictReader(input_tokentax_csv_file, delimiter=",", dialect=dialect)
        for tokentax_transaction_dict in tokentax_transaction_dictreader:
            logger.info("Transaction CSV: %r", tokentax_transaction_dict)
            transaction_list.append(TokenTaxTransaction.create_from_transaction_dict(tokentax_transaction_dict))

    for transaction in transaction_list:
        logger.info(transaction)

    # Successful exit
    sys.exit(0)
