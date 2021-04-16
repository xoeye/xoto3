from typing import Collection, Mapping, Tuple

from .errors import ItemNotYetFetchedError
from .modify import presume
from .prepare import add_item_to_base_request, parse_batch_get_request, prepare_clean_transaction
from .types import BatchGetItem, ItemKey, TransactionBuilder, VersionedTransaction


def lazy_batch_getting_transaction_builder(
    transaction_builder: TransactionBuilder,
    item_keys_by_table_name: Mapping[str, Collection[ItemKey]],
    batch_get_item: BatchGetItem,
) -> Tuple[VersionedTransaction, VersionedTransaction]:
    """If your transaction attempts to get/require an item that was not
    prefetched, we will stop, fetch it, and then retry your
    transaction from the beginning.

    Remember to make your transaction a pure function, or at least make
    sure all of its side effects are effectively idempotent!
    """
    # this outer loop lets us repeat the batch_get logic for actually
    # fetching needed items from the DynamoDB tables.
    while True:
        item_keys_by_table_name = parse_batch_get_request(item_keys_by_table_name)
        clean_transaction = prepare_clean_transaction(
            item_keys_by_table_name, batch_get_item(item_keys_by_table_name),
        )
        will_require_fetch = False
        # the goal of this loop is to accumulate as many lazy `get`s
        # as possible before actually executing them above.
        #
        # There isn't a general purpose way to 'know' about all the
        # gets that you might end up wanting to do, but if you have a
        # series of `get`s uninterrupted by `require`s, they will
        # function as a batch get.
        while True:
            try:
                built_transaction = transaction_builder(clean_transaction)
                if not will_require_fetch:
                    # the builder didn't need to ask for anything
                    return clean_transaction, built_transaction
                else:
                    # the builder has asked for everything it knows to ask for
                    # and we need to restart from the batch get above.
                    break
            except ItemNotYetFetchedError as nyf:
                assert nyf.key
                item_keys_by_table_name = add_item_to_base_request(
                    item_keys_by_table_name, (nyf.table_name, nyf.key),
                )
                if getattr(nyf, "force_immediate_fetch", False):
                    # perform immediate fetch; without it the transaction cannot proceed
                    break
                will_require_fetch = True
                # presume that the value is None for now, to let this `get` pass instead of excepting.
                clean_transaction = presume(clean_transaction, nyf.table_name, nyf.key, None)