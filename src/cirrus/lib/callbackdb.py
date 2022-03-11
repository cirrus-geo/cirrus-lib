import boto3
import logging
import os
import datetime

from typing import List, Literal
from boto3.dynamodb.conditions import Key, Attr

from cirrus.lib.statedb import STATES, IN_PROGRESS_STATES, FINAL_STATES


logger = logging.getLogger(__name__)

DYNAMO = boto3.resource('dynamodb')

STATES_LITERAL = Literal[tuple(STATES)]
FINAL_STATES_LITERAL = Literal[tuple(FINAL_STATES)]

TTL_ON_FINAL_STATE = datetime.timedelta(days=60)


def find_expiration_time() -> int:
    return int((datetime.datetime.now() + TTL_ON_FINAL_STATE).timestamp())


class CallbackDB:
    def __init__(self, table_name: str=os.getenv('CIRRUS_CALLBACK_DB', 'test')):
        """Initialize a CallbackDB instance using the Cirrus Callback DB table

        Args:
            table_name (str, optional): The Cirrus Callbakc DB Table name. Defaults to os.getenv('CIRRUS_CALLBACK_DB', None).
        """
        # initialize client
        self.db = DYNAMO
        self.table_name = table_name
        self.table = self.db.Table(table_name)

    def delete(self):
        # delete table (used for testing)
        self.table.delete()
        self.table.wait_until_not_exists()

    @staticmethod
    def payload_to_key(payload) -> str:
        """Construct the callback DB key from a ProcessPayload instance

        Args:
            payload (ProcessPayload): ProcessPayload instance

        Raises:

        Returns:
            str: callback DB key string
        """
        return '{}_{}_{}'.format(
            payload.process['workflow'],
            payload.collections_hash(),
            payload.items_hash(),
        )

    def create_callback(
        self,
        callback_token: str,
        payload_key: str,
        items: List[str],
        workflow_state: STATES_LITERAL,
    ) -> None:
        """Create a workflow callback record for a ProcessPayload

        Args:
            callback_token (str): the workflow callback token
            payload_key (str): the payload key for the ProcessPayload
            items (List[str]): the list of "collection/itemid" strings
                               representing the payload items
            workflow_state (str): current workflow state

        Returns:
            None
        """
        logger.debug("Creating callback for token '%s', payload '%s'", callback_token, payload_key)
        item = {
            'callback_token': callback_token,
            'collections_items': items,
            'workflow_collections256_itemids256': payload_key,
            'workflow_state': workflow_state,
        }

        if workflow_state in FINAL_STATES:
            item['expiration_time'] = find_expiration_time()

        response = self.table.put_item(
            Item=item,
            # prevents overwriting an existing entry
            ConditionExpression='attribute_not_exists(expiration_time)',
        )
        logger.debug("Created callback: %s", item)
        return response

    def create_callbacks(
        self,
        callback_tokens: str,
        payload_key: str,
        items: List[str],
        workflow_state: STATES_LITERAL,
    ) -> None:
        """Creates workflow callback records for a ProcessPayload from a list of tokens

        Args:
            callback_tokens (List[str]): the workflow callback tokens
            payload_key (str): the payload key for the ProcessPayload
            items (List[str]): the list of "collection/itemid" strings
                               representing the payload items
            workflow_state (str): current workflow state

        Returns:
            None
        """
        for token in callback_tokens:
            try:
                self.create_callback(token, payload_key, items, workflow_state)
            except Exception:
                logger.exception(
                    'Failed to create callback record: %s, %s, %s, %s',
                    token,
                    payload_key,
                    items,
                    workflow_state,
                )

    def get_payload_callback_tokens(self, payload_key: str, exclude_final_states: bool=True) -> List[str]:
        """Get all workflow callback tokens for a ProcessPayload

        Args:
            payload_key (str): the payload key for the ProcessPayload

        Keyword Args:
            exclude_final_states (bool): if True (default), return only
                                         tokens for records in non-final
                                         states

        Returns:
            List[str]: callback tokens
        """
        # if results are paginated,
        # we'll only query up to this
        # max number of pages
        MAX_PAGES = 100
        tokens = []

        key_cond = Key('workflow_collections256_itemids256').eq(payload_key)
        query = {
            'IndexName': 'by_payload',
            'KeyConditionExpression': key_cond,
        }

        if exclude_final_states:
            query['FilterExpression'] = Attr('workflow_state').is_in(IN_PROGRESS_STATES)

        page = 0
        while True:
            response = self.table.query(**query)

            tokens += [item['callback_token']['S'] for item in response['Items']]

            if not 'LastEvaluatedKey' in response:
                break
            if page >= MAX_PAGES:
                logger.error(
                    "More than %s pages of callbacks for payload '%s': cowardly refusing to continue dynamo queries",
                    MAX_PAGES,
                    payload_key,
                )
                break

            # we have more pages of results
            query['ExclusiveStartKey'] = response['LastEvaluatedKey']
            page += 1

        return tokens

    def set_final_state(self, callback_token: str, final_state: FINAL_STATES_LITERAL):
        """Update the workflow state of callback record with a final processing state

        Args:
            callback_token (str): the workflow callback token
            final_state (str): final workflow state

        Raises:
            ValueError on invalid final state

        Returns:
            dynamodb response object
        """
        if final_state not in FINAL_STATES:
            raise ValueError(f'Not a valid final workflow state: {final_state}')

        response = self.table.update_item(
            Key={'callback_token': callback_token},
            UpdateExpression=(
                'SET '
                'expiration_time = :expiration, '
                'workflow_state = :state'
            ),
            # prevents updates to an existing entry
            ConditionExpression='attribute_not_exists(expiration_time)',
            ExpressionAttributeValues={
                ':expiration': find_expiration_time(),
                ':state': final_state,
            }
        )
        logger.debug("Update callback token '%s' to state '%s'", callback_token, final_state)
        return response

    def set_final_state_multiple(
        self,
        callback_tokens: List[str],
        final_state: FINAL_STATES_LITERAL,
    ) -> None:
        """Update multiple callback records with a final processing state

        Args:
            callback_tokens (List[str]): the workflow callback tokens
            final_state (str): final workflow state

        Raises:
            ValueError on invalid final state

        Returns:
            None
        """
        if final_state not in FINAL_STATES:
            raise ValueError(f'Not a valid final workflow state: {final_state}')

        for token in callback_tokens:
            try:
                self.set_final_state(token, final_state)
            except Exception:
                logger.exception(
                    'Failed to set state on callback record: %s, %s',
                    token,
                    final_state,
                )
