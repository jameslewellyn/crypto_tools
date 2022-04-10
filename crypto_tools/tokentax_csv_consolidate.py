#!/usr/bin/env python3
"""Contains the main package functionality."""

from __future__ import annotations

import argparse
import copy
import csv
import enum
import locale
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Annotated, Dict, List, Literal, Optional, Sequence, Tuple, Union

from pydantic import BaseModel, Field

import yaml

locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
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
    buy_amount: Decimal
    buy_currency: str
    sell_amount: Decimal
    sell_currency: str
    fee_amount: Decimal
    fee_currency: str
    exchange: str
    exchange_id: ExchangeID
    group: str
    import_name: str
    comment: str
    date: datetime
    usd_equivalent: Decimal
    updated_at: datetime

    @staticmethod
    def create_from_transaction_dict(transaction_dict: Dict[str, str]) -> TokenTaxTransaction:
        """Initialize transaction from csv DictReader row."""
        return TokenTaxTransaction(
            TokenTaxTransactionType(transaction_dict["Type"]),
            Decimal(transaction_dict["BuyAmount"]),
            transaction_dict["BuyCurrency"],
            Decimal(transaction_dict["SellAmount"]),
            transaction_dict["SellCurrency"],
            Decimal(transaction_dict["FeeAmount"]),
            transaction_dict["FeeCurrency"],
            transaction_dict["Exchange"],
            ExchangeID.create_from_combined_string(transaction_dict["ExchangeId"]),
            transaction_dict["Group"],
            transaction_dict["Import"],
            transaction_dict["Comment"],
            datetime.strptime(transaction_dict["Date"], "%Y-%m-%dT%H:%M:%S.%fZ"),
            Decimal(locale.atof(transaction_dict["USDEquivalent"].strip(currency_symbol))),
            datetime.strptime(transaction_dict["UpdatedAt"], "%Y-%m-%dT%H:%M:%S.%fZ"),
        )


class AlterationTransactionPattern(BaseModel):
    """PyDantic class schema for individual transaction pattern."""

    transaction_type: str
    buy_currency: Optional[str]
    sell_currency: Optional[str]
    exchange: Optional[str]


class AlterationActionDoNothing(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["do_nothing"]

    def perform_on_transaction_list(
        self: AlterationActionDoNothing,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Do nothing."""
        return transaction_list


def find_fees_from_transaction_list(transaction_list: List[TokenTaxTransaction]) -> Tuple[Decimal, str]:
    """Find fees if any from transaction list."""
    has_fees_list: List[TokenTaxTransaction] = list()
    for transaction in transaction_list:
        if transaction.fee_amount != "" and transaction.fee_currency != "":
            has_fees_list.append(transaction)
    has_fees_list_length = len(has_fees_list)
    if has_fees_list_length == 0:
        return (Decimal(0), "")
    elif has_fees_list_length == 1:
        fees_transaction = has_fees_list[0]
        return (fees_transaction.fee_amount, fees_transaction.fee_currency)
    else:
        raise Exception("Cannot handle multiple sources of fees within the same transaction hash.")


def find_usd_equivalent_from_transaction_list(transaction_list: List[TokenTaxTransaction]) -> Decimal:
    """Find USD equivalent if any from transaction list."""
    has_usd_equivalent_list: List[TokenTaxTransaction] = list()
    for transaction in transaction_list:
        if transaction.usd_equivalent != "" and transaction.usd_equivalent != 0.0:
            has_usd_equivalent_list.append(transaction)
    has_usd_equivalent_list_length = len(has_usd_equivalent_list)
    if has_usd_equivalent_list_length == 0:
        return Decimal()
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
    if not all(
        transaction.date.replace(microsecond=0) == transaction_list[0].date.replace(microsecond=0)
        for transaction in transaction_list
    ):
        raise Exception("Cannot handle multiple Dates within the same transaction hash.")
    if not all(
        transaction.updated_at.replace(microsecond=0) == transaction_list[0].updated_at.replace(microsecond=0)
        for transaction in transaction_list
    ):
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


def separate_transactions_by_containing(
    transaction_list: List[TokenTaxTransaction],
    contains_token_list: Sequence[str],
) -> Tuple[List[TokenTaxTransaction], List[TokenTaxTransaction]]:
    """Create two sublists splitting by transactions with either only a buy or a sell component."""
    transactions_containing_list: List[TokenTaxTransaction] = list()
    transactions_without_list: List[TokenTaxTransaction] = list()
    for transaction in transaction_list:
        if transaction.buy_currency in contains_token_list or transaction.sell_currency in contains_token_list:
            transactions_containing_list.append(transaction)
        else:
            transactions_without_list.append(transaction)
    return (transactions_containing_list, transactions_without_list)


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


class AlterationActionConvertToSingleStakeTrades(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["convert_to_single_stake_trades"]
    unstaked_token: str
    staked_token: str

    def perform_on_transaction_list(
        self: AlterationActionDoNothing,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Convert transactions to list of trades."""
        stake_migration_transaction_list: List[TokenTaxTransaction] = list()
        for transaction in transaction_list:
            if transaction.buy_currency == self.unstaked_token:
                modified_transaction = copy.deepcopy(transaction)
                modified_transaction.transaction_type = TokenTaxTransactionType.TRADE
                modified_transaction.sell_amount = modified_transaction.buy_amount
                modified_transaction.sell_currency = self.staked_token
                stake_migration_transaction_list.append(modified_transaction)
            elif transaction.sell_currency == self.unstaked_token:
                modified_transaction = copy.deepcopy(transaction)
                modified_transaction.transaction_type = TokenTaxTransactionType.TRADE
                modified_transaction.buy_amount = modified_transaction.sell_amount
                modified_transaction.buy_currency = self.staked_token
                stake_migration_transaction_list.append(modified_transaction)
            else:
                stake_migration_transaction_list.append(transaction)
        return stake_migration_transaction_list


def convert_to_migrations(
    transaction_list: List[TokenTaxTransaction],
) -> List[TokenTaxTransaction]:
    """Merge 2 transactions into a migration transaction."""
    buy_only_list: List[TokenTaxTransaction] = list()
    sell_only_list: List[TokenTaxTransaction] = list()
    trade_list: List[TokenTaxTransaction] = list()
    converted_transaction_list: List[TokenTaxTransaction] = list()
    (fee_amount, fee_currency) = find_fees_from_transaction_list(transaction_list)
    usd_equivalent = find_usd_equivalent_from_transaction_list(transaction_list)
    ensure_common_elements_are_identical_in_transaction_list(transaction_list)
    for transaction in transaction_list:
        if transaction.transaction_type == TokenTaxTransactionType.TRADE:
            trade_list.append(transaction)
        elif transaction.buy_currency != "" and transaction.sell_currency == "":
            buy_only_list.append(transaction)
        elif transaction.buy_currency == "" and transaction.sell_currency != "":
            sell_only_list.append(transaction)
    buy_only_list_length = len(buy_only_list)
    sell_only_list_length = len(sell_only_list)
    if buy_only_list_length == 1 and sell_only_list_length == 1:
        buy_transaction = buy_only_list[0]
        sell_transaction = sell_only_list[0]
        converted_transaction_list.append(
            TokenTaxTransaction(
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
            ),
        )
    elif buy_only_list_length != 0 and sell_only_list_length != 0:
        raise Exception("Cannot merge Buy/Sell transactions to Migration unless there is one Buy and one Sell.")
    for transaction in trade_list:
        converted_trade_transaction = transaction
        converted_trade_transaction.transaction_type = TokenTaxTransactionType.MIGRATION
        converted_transaction_list.append(converted_trade_transaction)
    return converted_transaction_list


class AlterationActionConvertToMigrations(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["convert_to_migrations"]
    rewards: Optional[Sequence[str]]

    def perform_on_transaction_list(
        self: AlterationActionDoNothing,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Convert transactions to a migration."""
        if self.rewards is None:
            return convert_to_migrations(transaction_list)
        else:
            (rewards_list, migration_list) = separate_transactions_by_containing(transaction_list, self.rewards)
            for rewards_transaction in rewards_list:
                rewards_transaction.transaction_type = TokenTaxTransactionType.STAKING
            return rewards_list + convert_to_migrations(migration_list)


class AlterationActionConvertToStaking(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["convert_to_staking"]

    def perform_on_transaction_list(
        self: AlterationActionDoNothing,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Convert transactions to a staking."""
        staking_transaction_list: List[TokenTaxTransaction] = list()
        for transaction in transaction_list:
            if transaction.transaction_type != TokenTaxTransactionType.DEPOSIT:
                raise Exception("Found transaction type other than Deposit when converting to Staking.")
            else:
                staking_transaction = transaction
                staking_transaction.transaction_type = TokenTaxTransactionType.STAKING
                staking_transaction_list.append(staking_transaction)
        return staking_transaction_list


def convert_to_stake_migrations(
    transaction_list: List[TokenTaxTransaction],
    unstaked_token: str,
    staked_token: str,
) -> List[TokenTaxTransaction]:
    """P yDantic class schema for actions to be taken, in order, on the list of transactions."""
    stake_migration_transaction_list: List[TokenTaxTransaction] = list()
    for transaction in transaction_list:
        if transaction.buy_currency == unstaked_token:
            modified_transaction = copy.deepcopy(transaction)
            modified_transaction.transaction_type = TokenTaxTransactionType.MIGRATION
            modified_transaction.sell_amount = modified_transaction.buy_amount
            modified_transaction.sell_currency = staked_token
            stake_migration_transaction_list.append(modified_transaction)
        elif transaction.sell_currency == unstaked_token:
            modified_transaction = copy.deepcopy(transaction)
            modified_transaction.transaction_type = TokenTaxTransactionType.MIGRATION
            modified_transaction.buy_amount = modified_transaction.sell_amount
            modified_transaction.buy_currency = staked_token
            stake_migration_transaction_list.append(modified_transaction)
        else:
            stake_migration_transaction_list.append(transaction)
    return stake_migration_transaction_list


class AlterationActionConvertToStakeMigration(BaseModel):
    """Pyantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["convert_to_stake_migration"]
    unstaked_token: str
    staked_token: str
    rewards: Optional[Sequence[str]]

    def perform_on_transaction_list(
        self: AlterationActionDoNothing,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Convert transactions to a staking."""
        if self.rewards is None:
            return convert_to_stake_migrations(transaction_list, self.unstaked_token, self.staked_token)
        else:
            (rewards_list, migration_list) = separate_transactions_by_containing(transaction_list, self.rewards)
            for rewards_transaction in rewards_list:
                rewards_transaction.transaction_type = TokenTaxTransactionType.STAKING
            return rewards_list + convert_to_stake_migrations(migration_list, self.unstaked_token, self.staked_token)


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


class AlterationActionKeepOnlyTypes(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["keep_only_types"]
    keeps: Sequence[str]

    def perform_on_transaction_list(
        self: AlterationActionKeepOnlyTypes,
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
        self: AlterationActionKeepOnlyTypes,
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
            if transaction.buy_currency not in self.removes and transaction.sell_currency not in self.removes:
                updated_transaction_list.append(transaction)
        return updated_transaction_list


class AlterationActionMergeSameCurrency(BaseModel):
    """PyDantic class schema for actions to be taken, in order, on the list of transactions."""

    name: Literal["merge_same_currency"]
    merge_currency: str

    def perform_on_transaction_list(
        self: AlterationActionRemoveContaining,
        transaction_list: List[TokenTaxTransaction],
    ) -> List[TokenTaxTransaction]:
        """Remove all transactions except listed types."""
        mergable_transaction_list: List[TokenTaxTransaction] = list()
        modified_transaction_list: List[TokenTaxTransaction] = list()
        for transaction in transaction_list:
            if transaction.buy_currency == self.merge_currency or transaction.sell_currency == self.merge_currency:
                mergable_transaction_list.append(transaction)
            else:
                modified_transaction_list.append(copy.deepcopy(transaction))
        net_merged_value = Decimal()
        if len(mergable_transaction_list) == 0:
            raise Exception("No mergeable transactions found for token %s.", self.merge_currency)
        (fee_amount, fee_currency) = find_fees_from_transaction_list(mergable_transaction_list)
        usd_equivalent = find_usd_equivalent_from_transaction_list(mergable_transaction_list)
        ensure_common_elements_are_identical_in_transaction_list(mergable_transaction_list)
        for mergable_transaction in mergable_transaction_list:
            if mergable_transaction.buy_amount != 0.0 and mergable_transaction.sell_amount != 0.0:
                raise Exception("Cannot merge in a transaction with both a buy and sell component")
            elif mergable_transaction.buy_amount != 0.0:
                net_merged_value += mergable_transaction.buy_amount
            elif mergable_transaction.sell_amount != 0.0:
                net_merged_value -= mergable_transaction.sell_amount
        if net_merged_value > 0:
            modified_transaction_list.append(
                TokenTaxTransaction(
                    TokenTaxTransactionType.DEPOSIT,
                    net_merged_value,
                    self.merge_currency,
                    Decimal(0),
                    "",
                    fee_amount,
                    fee_currency,
                    mergable_transaction_list[0].exchange,
                    mergable_transaction_list[0].exchange_id,
                    mergable_transaction_list[0].group,
                    mergable_transaction_list[0].import_name,
                    mergable_transaction_list[0].comment,
                    mergable_transaction_list[0].date,
                    usd_equivalent,
                    mergable_transaction_list[0].updated_at,
                ),
            )
        elif net_merged_value < 0:
            modified_transaction_list.append(
                TokenTaxTransaction(
                    TokenTaxTransactionType.WITHDRAWAL,
                    Decimal(0),
                    "",
                    abs(net_merged_value),
                    self.merge_currency,
                    fee_amount,
                    fee_currency,
                    mergable_transaction_list[0].exchange,
                    mergable_transaction_list[0].exchange_id,
                    mergable_transaction_list[0].group,
                    mergable_transaction_list[0].import_name,
                    mergable_transaction_list[0].comment,
                    mergable_transaction_list[0].date,
                    usd_equivalent,
                    mergable_transaction_list[0].updated_at,
                ),
            )
        else:
            raise Exception("Merge cancelled out completely.")
        return modified_transaction_list


AlterationActionUnion = Annotated[
    Union[
        AlterationActionDoNothing,
        AlterationActionConvertToTrades,
        AlterationActionConvertToSingleStakeTrades,
        AlterationActionConvertToMigrations,
        AlterationActionConvertToStaking,
        AlterationActionConvertToStakeMigration,
        AlterationActionKeepOnlyTypes,
        AlterationActionRenameToken,
        AlterationActionRemoveContaining,
        AlterationActionMergeSameCurrency,
    ],
    Field(discriminator="name"),
]


class Alteration(BaseModel):
    """PyDantic class schema for sets of before and after transaction pattern lists."""

    tx_hash: Optional[str]
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
    # logger.debug("tx type: %s", tokentax_transaction.transaction_type.value)
    # logger.debug("tx buyc: %s", tokentax_transaction.buy_currency)
    # logger.debug("tx sellc: %s", tokentax_transaction.sell_currency)
    # logger.debug("tx exchange: %s", tokentax_transaction.exchange)
    # logger.debug("alt type: %s", alteration_transaction_pattern.transaction_type)
    # logger.debug("alt buyc: %s", alteration_transaction_pattern.buy_currency)
    # logger.debug("alt sellc: %s", alteration_transaction_pattern.sell_currency)
    # logger.debug("alt exchange: %s", alteration_transaction_pattern.exchange)
    common_elements: int = 0
    if alteration_transaction_pattern.transaction_type is not None:
        if tokentax_transaction.transaction_type.value == alteration_transaction_pattern.transaction_type:
            logger.debug("Transaction and alteration transaction pattern have same transaction type.")
            common_elements += 1
        else:
            logger.debug("Transaction does not match alteration transaction pattern.")
            return False
    if alteration_transaction_pattern.buy_currency is not None:
        if tokentax_transaction.buy_currency == alteration_transaction_pattern.buy_currency:
            logger.debug("Transaction and alteration transaction pattern have same buy currency.")
            common_elements += 1
        else:
            logger.debug("Transaction does not match alteration transaction pattern.")
            return False
    if alteration_transaction_pattern.sell_currency is not None:
        if tokentax_transaction.sell_currency == alteration_transaction_pattern.sell_currency:
            logger.debug("Transaction and alteration transaction pattern have same sell currency.")
            common_elements += 1
        else:
            logger.debug("Transaction does not match alteration transaction pattern.")
            return False
    if alteration_transaction_pattern.exchange is not None:
        if tokentax_transaction.exchange == alteration_transaction_pattern.exchange:
            logger.debug("Transaction and alteration transaction pattern have same Exchange.")
            common_elements += 1
    if common_elements == 0:
        logger.debug("No common elements found between transaction and alteration pattern.")
        return False
    else:
        logger.debug("Transaction matches alteration transaction pattern with % d common elements.", common_elements)
        return True


def remove_transaction_from_tx_pattern_list(
    transaction: TokenTaxTransaction,
    tx_patterns: List[AlterationTransactionPattern],
) -> bool:
    """Determine if transaction is in before-pattern list."""
    for tx_pattern in tx_patterns:
        if compare_tokentax_transaction_to_alteration_tx_pattern(transaction, tx_pattern):
            tx_patterns.remove(tx_pattern)
            return True
    else:
        return False


def all_transactions_in_tx_pattern_list(
    transaction_list: List[TokenTaxTransaction],
    tx_patterns: List[AlterationTransactionPattern],
) -> bool:
    """Determine if all transaction are in before-pattern list."""
    tx_patterns_copy = copy.deepcopy(tx_patterns)
    for transaction in transaction_list:
        if not remove_transaction_from_tx_pattern_list(transaction, tx_patterns_copy):
            return False
    else:
        return True


def match_transaction_list_to_alteration(
    alterations_mapping: AlterationsMapping,
    transaction_hash: str,
    transaction_list: List[TokenTaxTransaction],
) -> Optional[Alteration]:
    """Return an alteration pattern if possible for the given transaction list."""
    transaction_list_length = len(transaction_list)
    for alteration in alterations_mapping.alterations:
        if alteration.tx_hash is not None and transaction_hash != alteration.tx_hash:
            continue
        if len(alteration.tx_patterns) == transaction_list_length:
            if all_transactions_in_tx_pattern_list(transaction_list, alteration.tx_patterns):
                logger.debug("Found alteration matching input transaction: %r", alteration)
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
    logger.info("")


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
    console_handler = logging.StreamHandler(sys.stdout)
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
        logger.info("__________________________________________________")
        quick_print_transactions_same_hash_list(separated_transaction_list)
        same_transaction_hash_count = len(separated_transaction_list)
        if same_transaction_hash_count not in input_transaction_hash_count_dict:
            input_transaction_hash_count_dict[same_transaction_hash_count] = 0
        input_transaction_hash_count_dict[same_transaction_hash_count] += 1
        alteration = match_transaction_list_to_alteration(
            alterations_mapping,
            transaction_hash,
            separated_transaction_list,
        )
        if alteration is not None:
            if same_transaction_hash_count not in output_transaction_hash_count_dict:
                output_transaction_hash_count_dict[same_transaction_hash_count] = 0
            output_transaction_hash_count_dict[same_transaction_hash_count] += 1
            modified_transaction_list = copy.deepcopy(separated_transaction_list)
            for action in alteration.actions:
                modified_transaction_list = action.perform_on_transaction_list(modified_transaction_list)
            output_transaction_list += modified_transaction_list
            quick_print_transactions_same_hash_list(modified_transaction_list)
        elif same_transaction_hash_count == 1:
            output_transaction_list.append(separated_transaction_list[0])
            quick_print_transactions_same_hash_list(separated_transaction_list)
            if same_transaction_hash_count not in output_transaction_hash_count_dict:
                output_transaction_hash_count_dict[same_transaction_hash_count] = 0
            output_transaction_hash_count_dict[same_transaction_hash_count] += 1
        else:
            if same_transaction_hash_count not in no_match_transaction_hash_count_dict:
                no_match_transaction_hash_count_dict[same_transaction_hash_count] = 0
            no_match_transaction_hash_count_dict[same_transaction_hash_count] += 1
            logger.debug(
                "No alteration found for %d transactions with hash %s.",
                same_transaction_hash_count,
                transaction_hash,
            )
            quick_print_transactions_same_hash_list(separated_transaction_list)
        logger.info("__________________________________________________")

    # logger.info("")
    logger.info("------------------------")
    logger.info("INPUT COUNTS: %r", input_transaction_hash_count_dict)
    logger.info("NOMATCH COUNTS: %r", no_match_transaction_hash_count_dict)
    logger.info("OUTPUT PAIRED COUNTS: %r", output_transaction_hash_count_dict)

    logger.info("------------------------")

    # Successful exit
    sys.exit(0)
