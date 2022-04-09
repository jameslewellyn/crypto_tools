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
from typing import Annotated, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import BaseModel, Field

import yaml

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
class ExchangeID:
    """Holds the pieces of the Exchange ID information."""

    transaction_hash: str
    transaction_suffix: List[str]

    @staticmethod
    def create_from_combined_string(combined_string: str) -> ExchangeID:
        """Initialize from combined string."""
        return ExchangeID(
            combined_string.split("-")[0],
            combined_string.split("-")[1:],
        )


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
    exchange_id: ExchangeID
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
            ExchangeID.create_from_combined_string(transaction_dict["ExchangeId"]),
            transaction_dict["Group"],
            transaction_dict["Import"],
            transaction_dict["Comment"],
            datetime.strptime(transaction_dict["Date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
            locale.atof(transaction_dict["USDEquivalent"].strip(currency_symbol)),
            datetime.strptime(transaction_dict["UpdatedAt"], "%Y-%m-%dT%H:%M:%S.%fZ"),
        )


class AlterationTransactionPattern(BaseModel):
    """PyDantic class schema for individual transaction pattern."""

    transaction_type: str
    buy_currency: Optional[str]
    sell_currency: Optional[str]


class AlterationActionDoNothing(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["do_nothing"]

    def perform_on_transaction_list(
        self: AlterationActionDoNothing,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Do nothing."""
        return transaction_list


def find_fees_from_transaction_list(transaction_list: List[TokenTaxTransaction]) -> Tuple[float, str]:
    """Find fees if any from transaction list."""
    has_fees_list: List[TokenTaxTransaction] = list()
    for transaction in transaction_list:
        if transaction.fee_amount != "" and transaction.fee_currency != "":
            has_fees_list.append(transaction)
    has_fees_list_length = len(has_fees_list)
    if has_fees_list_length == 0:
        return (0.0, "")
    elif has_fees_list_length == 1:
        fees_transaction = has_fees_list[0]
        return (fees_transaction.fee_amount, fees_transaction.fee_currency)
    else:
        raise Exception("Cannot handle multiple sources of fees within the same transaction hash.")


def find_usd_equivalent_from_transaction_list(transaction_list: List[TokenTaxTransaction]) -> float:
    """Find USD equivalent if any from transaction list."""
    has_usd_equivalent_list: List[TokenTaxTransaction] = list()
    for transaction in transaction_list:
        if transaction.usd_equivalent != "" and transaction.usd_equivalent != 0.0:
            has_usd_equivalent_list.append(transaction)
    has_usd_equivalent_list_length = len(has_usd_equivalent_list)
    if has_usd_equivalent_list_length == 0:
        return 0.0
    elif has_usd_equivalent_list_length == 1:
        usd_equivalent_transaction = has_usd_equivalent_list[0]
        return usd_equivalent_transaction.usd_equivalent
    else:
        raise Exception("Cannot handle multiple sources of USD Equivalent within the same transaction hash.")


def ensure_common_elements_are_identical_in_transaction_list(transaction_list: List[TokenTaxTransaction]) -> None:
    """Throw an Exception if any of the fields that are supposed to be identical are different."""
    if not all(transaction.exchange == transaction_list[0].exchange for transaction in transaction_list):
        raise Exception("Cannot handle multiple Exchanges within the same transaction hash.")
    if not all(
        transaction.exchange_id.transaction_hash == transaction_list[0].exchange_id.transaction_hash
        for transaction in transaction_list
    ):
        raise Exception("Cannot handle multiple Exchange IDs within the same transaction hash.")
    if not all(transaction.group == transaction_list[0].group for transaction in transaction_list):
        raise Exception("Cannot handle multiple Groups within the same transaction hash.")
    if not all(transaction.import_name == transaction_list[0].import_name for transaction in transaction_list):
        raise Exception("Cannot handle multiple Import Names within the same transaction hash.")
    if not all(transaction.comment == transaction_list[0].comment for transaction in transaction_list):
        raise Exception("Cannot handle multiple Comments within the same transaction hash.")
    if not all(transaction.date == transaction_list[0].date for transaction in transaction_list):
        raise Exception("Cannot handle multiple Dates within the same transaction hash.")
    if not all(transaction.updated_at == transaction_list[0].updated_at for transaction in transaction_list):
        raise Exception("Cannot handle multiple Updated At Times within the same transaction hash.")


def separate_buy_from_sell_transactions(
    transaction_list: List[TokenTaxTransaction],
) -> Tuple[List[TokenTaxTransaction], List[TokenTaxTransaction]]:
    """Create two sublists splitting by transactions with either only a buy or a sell component."""
    buy_only_list: List[TokenTaxTransaction] = list()
    sell_only_list: List[TokenTaxTransaction] = list()
    for transaction in transaction_list:
        if transaction.buy_currency != "" and transaction.sell_currency == "":
            buy_only_list.append(transaction)
        elif transaction.buy_currency == "" and transaction.sell_currency != "":
            sell_only_list.append(transaction)
        else:
            raise Exception("Attmepting to make Trade transaction, but found a transaction with both buy and sell.")
    return (buy_only_list, sell_only_list)


def convert_to_trades(
    transaction_list: List[TokenTaxTransaction],
) -> List[TokenTaxTransaction]:
    """Merge any number of transaction into a set of trade transactions."""
    converted_trade_list: List[TokenTaxTransaction] = list()
    (fee_amount, fee_currency) = find_fees_from_transaction_list(transaction_list)
    ensure_common_elements_are_identical_in_transaction_list(transaction_list)
    (buy_only_list, sell_only_list) = separate_buy_from_sell_transactions(transaction_list)
    buy_only_list_length = len(buy_only_list)
    sell_only_list_length = len(sell_only_list)
    if buy_only_list_length == 1 and sell_only_list_length == 1:
        buy_transaction = buy_only_list[0]
        sell_transaction = sell_only_list[0]
        usd_equivalent = find_usd_equivalent_from_transaction_list(transaction_list)
        converted_trade_list.append(
            TokenTaxTransaction(
                TokenTaxTransactionType.TRADE,
                buy_transaction.buy_amount,
                buy_transaction.buy_currency,
                sell_transaction.sell_amount,
                sell_transaction.sell_currency,
                fee_amount,
                fee_currency,
                buy_transaction.exchange,
                buy_transaction.exchange_id,
                buy_transaction.group,
                buy_transaction.import_name,
                buy_transaction.comment,
                buy_transaction.date,
                usd_equivalent,
                buy_transaction.updated_at,
            ),
        )
    elif buy_only_list_length == 1 and sell_only_list_length > 1:
        buy_transaction = buy_only_list[0]
        original_buy_amount = buy_transaction.buy_amount
        split_buy_amount = original_buy_amount / sell_only_list_length
        usd_equivalent = find_usd_equivalent_from_transaction_list(transaction_list)
        split_usd_equivalent = usd_equivalent / sell_only_list_length
        split_fee_amount = fee_amount / sell_only_list_length
        for sell_transaction in sell_only_list:
            converted_trade_list.append(
                TokenTaxTransaction(
                    TokenTaxTransactionType.TRADE,
                    split_buy_amount,
                    buy_transaction.buy_currency,
                    sell_transaction.sell_amount,
                    sell_transaction.sell_currency,
                    split_fee_amount,
                    fee_currency,
                    buy_transaction.exchange,
                    buy_transaction.exchange_id,
                    buy_transaction.group,
                    buy_transaction.import_name,
                    buy_transaction.comment,
                    buy_transaction.date,
                    split_usd_equivalent,
                    buy_transaction.updated_at,
                ),
            )
    elif buy_only_list_length > 1 and sell_only_list_length == 1:
        sell_transaction = sell_only_list[0]
        original_sell_amount = sell_transaction.sell_amount
        split_sell_amount = original_sell_amount / buy_only_list_length
        split_fee_amount = fee_amount / buy_only_list_length
        for buy_transaction in buy_only_list:
            converted_trade_list.append(
                TokenTaxTransaction(
                    TokenTaxTransactionType.TRADE,
                    buy_transaction.buy_amount,
                    buy_transaction.buy_currency,
                    split_sell_amount,
                    sell_transaction.sell_currency,
                    split_fee_amount,
                    fee_currency,
                    sell_transaction.exchange,
                    sell_transaction.exchange_id,
                    sell_transaction.group,
                    sell_transaction.import_name,
                    sell_transaction.comment,
                    sell_transaction.date,
                    buy_transaction.usd_equivalent,
                    sell_transaction.updated_at,
                ),
            )
    else:
        raise Exception("Neither buy_only_list_length nor sell_only_list_length can be 0.")
    return converted_trade_list


class AlterationActionConvertToTrades(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["convert_to_trades"]

    def perform_on_transaction_list(
        self: AlterationActionDoNothing,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Convert transactions to list of trades."""
        return convert_to_trades(transaction_list)


def convert_to_migration(
    transaction_list: List[TokenTaxTransaction],
) -> List[TokenTaxTransaction]:
    """Merge 2 transactions into a migration transaction."""
    buy_only_list: List[TokenTaxTransaction] = list()
    sell_only_list: List[TokenTaxTransaction] = list()
    migration_transaction: TokenTaxTransaction
    (fee_amount, fee_currency) = find_fees_from_transaction_list(transaction_list)
    usd_equivalent = find_usd_equivalent_from_transaction_list(transaction_list)
    ensure_common_elements_are_identical_in_transaction_list(transaction_list)
    for transaction in transaction_list:
        if transaction.buy_currency != "" and transaction.sell_currency == "":
            buy_only_list.append(transaction)
        if transaction.buy_currency == "" and transaction.sell_currency != "":
            sell_only_list.append(transaction)
    buy_only_list_length = len(buy_only_list)
    sell_only_list_length = len(sell_only_list)
    if buy_only_list_length == 1 and sell_only_list_length == 1:
        buy_transaction = buy_only_list[0]
        sell_transaction = sell_only_list[0]
        migration_transaction = TokenTaxTransaction(
            TokenTaxTransactionType.MIGRATION,
            buy_transaction.buy_amount,
            buy_transaction.buy_currency,
            sell_transaction.sell_amount,
            sell_transaction.sell_currency,
            fee_amount,
            fee_currency,
            buy_transaction.exchange,
            buy_transaction.exchange_id,
            buy_transaction.group,
            buy_transaction.import_name,
            buy_transaction.comment,
            buy_transaction.date,
            usd_equivalent,
            buy_transaction.updated_at,
        )
    else:
        raise Exception("Cannot merge transactions to Migration unless there is one Buy and one Sell.")
    return [migration_transaction]


class AlterationActionConvertToMigration(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["convert_to_migration"]

    def perform_on_transaction_list(
        self: AlterationActionDoNothing,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Convert transactions to a migration."""
        return convert_to_migration(transaction_list)


def keep_transactions_by_type(
    transaction_list: List[TokenTaxTransaction],
    keep_transaction_type_list: List[TokenTaxTransactionType],
) -> List[TokenTaxTransaction]:
    """Keep only transactions of the specified type."""
    kept_transaction_list: List[TokenTaxTransaction] = list()
    for transaction in transaction_list:
        if transaction.transaction_type in keep_transaction_type_list:
            kept_transaction_list.append(transaction)
    return kept_transaction_list


class AlterationActionKeepOnly(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["keep_only"]
    keeps: Sequence[str]

    def perform_on_transaction_list(
        self: AlterationActionKeepOnly,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Remove all transactions except listed types."""
        transaction_type_list: List[TokenTaxTransactionType] = list()
        for keep in self.keeps:
            transaction_type_list.append(TokenTaxTransactionType(keep))
        return keep_transactions_by_type(transaction_list, transaction_type_list)


class AlterationActionRenameToken(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["rename_to"]
    token_name: str
    rename_to: str

    def perform_on_transaction_list(
        self: AlterationActionKeepOnly,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Remove all transactions except listed types."""
        for transaction in transaction_list:
            if transaction.buy_currency == self.token_name:
                transaction.buy_currency = self.rename_to
            if transaction.sell_currency == self.token_name:
                transaction.sell_currency = self.rename_to
        return transaction_list


class AlterationActionRemoveContaining(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["remove_containing"]
    removes: Sequence[str]

    def perform_on_transaction_list(
        self: AlterationActionRemoveContaining,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Remove all transactions except listed types."""
        updated_transaction_list: List[TokenTaxTransaction] = list()
        for transaction in transaction_list:
            updated_transaction_list.append(transaction)
        return updated_transaction_list


AlterationActionUnion = Annotated[
    Union[
        AlterationActionDoNothing,
        AlterationActionConvertToTrades,
        AlterationActionConvertToMigration,
        AlterationActionKeepOnly,
        AlterationActionRenameToken,
        AlterationActionRemoveContaining,
    ],
    Field(discriminator="name"),
]


class Alteration(BaseModel):
    """PyDantic class schema for sets of before and after transaction pattern lists."""

    tx_patterns: List[AlterationTransactionPattern]
    actions: List[AlterationActionUnion]


class AlterationsMapping(BaseModel):
    """PyDantic class schema for overall YAML file."""

    alterations: List[Alteration]


def compare_tokentax_transaction_to_alteration_tx_pattern(
    tokentax_transaction: TokenTaxTransaction,
    alteration_transaction_pattern: AlterationTransactionPattern,
) -> bool:
    """Compare two objects for equality."""
    logger.debug("tx type: %s", tokentax_transaction.transaction_type.value)
    logger.debug("tx buyc: %s", tokentax_transaction.buy_currency)
    logger.debug("tx sellc: %s", tokentax_transaction.sell_currency)
    logger.debug("alt type: %s", alteration_transaction_pattern.transaction_type)
    logger.debug("alt buyc: %s", alteration_transaction_pattern.buy_currency)
    logger.debug("alt sellc: %s", alteration_transaction_pattern.sell_currency)
    if (
        (tokentax_transaction.transaction_type.value == alteration_transaction_pattern.transaction_type)
        and (
            (tokentax_transaction.buy_currency == alteration_transaction_pattern.buy_currency)
            or ((tokentax_transaction.buy_currency == "") and (alteration_transaction_pattern.buy_currency is None))
        )
        and (
            (tokentax_transaction.sell_currency == alteration_transaction_pattern.sell_currency)
            or ((tokentax_transaction.sell_currency == "") and (alteration_transaction_pattern.sell_currency is None))
        )
    ):
        logger.debug("Matched before-pattern in alteration.")
        return True
    else:
        logger.debug("No-match before-pattern in alteration.")
        logger.debug("Transaction: %r", tokentax_transaction)
        logger.debug("Before pattern: %r", alteration_transaction_pattern)
        return False


def transaction_in_tx_pattern_list(
    transaction: TokenTaxTransaction,
    tx_patterns: List[AlterationTransactionPattern],
) -> bool:
    """Determine if transaction is in before-pattern list."""
    for tx_pattern in tx_patterns:
        if compare_tokentax_transaction_to_alteration_tx_pattern(transaction, tx_pattern):
            return True
    else:
        return False


def all_transactions_in_tx_pattern_list(
    transaction_list: List[TokenTaxTransaction],
    tx_patterns: List[AlterationTransactionPattern],
) -> bool:
    """Determine if all transaction are in before-pattern list."""
    for transaction in transaction_list:
        if not transaction_in_tx_pattern_list(transaction, tx_patterns):
            return False
    else:
        return True


def match_transaction_list_to_alteration(
    alterations_mapping: AlterationsMapping,
    transaction_list: List[TokenTaxTransaction],
) -> Optional[Alteration]:
    """Return an alteration pattern if possible for the given transaction list."""
    transaction_list_length = len(transaction_list)
    for alteration in alterations_mapping.alterations:
        if len(alteration.tx_patterns) == transaction_list_length:
            if all_transactions_in_tx_pattern_list(transaction_list, alteration.tx_patterns):
                logger.debug("Found alteration matching input transaction.")
                return alteration
    else:
        return None


def quick_print_transactions_same_hash_list(transaction_list: List[TokenTaxTransaction]) -> None:
    """Print important info for easy viewing."""
    logger.info("---%s", transaction_list[0].exchange_id.transaction_hash)
    for (i, transaction) in enumerate(transaction_list):
        logger.info(
            "#%d) %s: BUY = %s:%f, SELL = %s:%f",
            i,
            transaction.transaction_type.value,
            transaction.buy_currency,
            transaction.buy_amount,
            transaction.sell_currency,
            transaction.sell_amount,
        )


def main() -> None:
    """Do the main thing."""
    # Get arguments
    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Show DEBUG level logging messages")
    parser.add_argument("input_tokentax_csv_file", type=str, help="Path to TokenTax CSV file to manipulate")
    parser.add_argument("output_tokentax_csv_file", type=str, help="Path to place modified TokenTax CSV file")
    parser.add_argument("alterations_yaml_file", type=str, help="Path to YAML file detailings alteration patterns")
    args = parser.parse_args()

    # Use root python logger to set loglevel
    if args.verbose is True:
        package_logger.setLevel(logging.DEBUG)
    else:
        package_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    logger_formatter = logging.Formatter("%(message)s")
    console_handler.setFormatter(logger_formatter)
    package_logger.addHandler(console_handler)

    input_tokentax_csv_file_path = Path(args.input_tokentax_csv_file)
    if not input_tokentax_csv_file_path.exists():
        logger.error("Input TokenTax CSV file does not exist at location: %s", input_tokentax_csv_file_path)
        sys.exit(1)
    output_tokentax_csv_file_path = Path(args.output_tokentax_csv_file)
    if not output_tokentax_csv_file_path.parent.exists():
        logger.error("Output TokenTax CSV parent directory does not exist: %s", output_tokentax_csv_file_path.parent)
        sys.exit(2)
    alterations_yaml_file_path = Path(args.alterations_yaml_file)
    if not alterations_yaml_file_path.exists():
        logger.error("Alterations YAML file does not exist at location: %s", alterations_yaml_file_path)
        sys.exit(3)
    with open(alterations_yaml_file_path, "r") as alterations_yaml_contents:
        alterations_yaml_dict = yaml.safe_load(alterations_yaml_contents)
        alterations_mapping = AlterationsMapping(**alterations_yaml_dict)

    transaction_list: List[TokenTaxTransaction] = list()
    with input_tokentax_csv_file_path.open("r") as input_tokentax_csv_file:
        dialect = csv.Sniffer().sniff(input_tokentax_csv_file.read(1024))
        input_tokentax_csv_file.seek(0)
        tokentax_transaction_dictreader = csv.DictReader(input_tokentax_csv_file, delimiter=",", dialect=dialect)
        for tokentax_transaction_dict in tokentax_transaction_dictreader:
            logger.debug("Transaction CSV: %r", tokentax_transaction_dict)
            transaction_list.append(
                TokenTaxTransaction.create_from_transaction_dict(tokentax_transaction_dict),
            )

    separated_transaction_list_of_lists: Dict["str", List[TokenTaxTransaction]] = dict()
    for transaction in transaction_list:
        transaction_hash = transaction.exchange_id.transaction_hash
        if transaction_hash not in separated_transaction_list_of_lists:
            separated_transaction_list_of_lists[transaction_hash] = []
        separated_transaction_list_of_lists[transaction_hash].append(transaction)

    input_transaction_hash_count_dict: Dict[int, int] = dict()
    no_match_transaction_hash_count_dict: Dict[int, int] = dict()
    output_transaction_hash_count_dict: Dict[int, int] = dict()
    output_transaction_list: List[TokenTaxTransaction] = list()
    for (transaction_hash, separated_transaction_list) in separated_transaction_list_of_lists.items():
        # logger.info("__________________________________________________")
        # quick_print_transactions_same_hash_list(separated_transaction_list)
        same_transaction_hash_count = len(separated_transaction_list)
        if same_transaction_hash_count not in input_transaction_hash_count_dict:
            input_transaction_hash_count_dict[same_transaction_hash_count] = 0
        input_transaction_hash_count_dict[same_transaction_hash_count] += 1
        if same_transaction_hash_count == 1:
            # output_transaction_list.append(separated_transaction_list[0])
            # quick_print_transactions_same_hash_list(separated_transaction_list)
            if same_transaction_hash_count not in output_transaction_hash_count_dict:
                output_transaction_hash_count_dict[same_transaction_hash_count] = 0
            output_transaction_hash_count_dict[same_transaction_hash_count] += 1
        else:
            alteration = match_transaction_list_to_alteration(alterations_mapping, separated_transaction_list)
            if alteration is None:
                if same_transaction_hash_count not in no_match_transaction_hash_count_dict:
                    no_match_transaction_hash_count_dict[same_transaction_hash_count] = 0
                no_match_transaction_hash_count_dict[same_transaction_hash_count] += 1
                logger.debug(
                    "No alteration found for %d transactions with hash %s.",
                    same_transaction_hash_count,
                    transaction_hash,
                )
                quick_print_transactions_same_hash_list(separated_transaction_list)
            else:
                if same_transaction_hash_count not in output_transaction_hash_count_dict:
                    output_transaction_hash_count_dict[same_transaction_hash_count] = 0
                output_transaction_hash_count_dict[same_transaction_hash_count] += 1
                modified_transaction_list = separated_transaction_list
                for action in alteration.actions:
                    modified_transaction_list = action.perform_on_transaction_list(modified_transaction_list)
                output_transaction_list += modified_transaction_list
                # quick_print_transactions_same_hash_list(modified_transaction_list)
        # logger.info("__________________________________________________")

    logger.info("INPUT COUNTS: %r", input_transaction_hash_count_dict)
    logger.info("NOMATCH COUNTS: %r", no_match_transaction_hash_count_dict)
    logger.info("OUTPUT PAIRED COUNTS: %r", output_transaction_hash_count_dict)

    logger.info("------------------------")

    # Successful exit
    sys.exit(0)
