"""
Etherscan Component main class.

"""
import csv
import logging
from typing import List, Dict, Any

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

import eth_utils
import etherscan

# configuration variables
KEY_ADDRESS = 'address'
KEY_START_BLOCK = 'start_block'
KEY_END_BLOCK = 'end_block'
KEY_PAGE = 'page'
KEY_OFFSET = 'offset'
KEY_SORT = 'sort'
KEY_API_KEY = '#api_key'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_ADDRESS, KEY_API_KEY]


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self) -> None:
        super().__init__()

    @staticmethod
    def to_checksum(address: str) -> str:
        """
        Helper method to check if the ethereum address is a valid checksum address, if not then try to force
        checksum.
        """
        if eth_utils.is_checksum_address(address):
            return address
        else:
            return str(eth_utils.to_checksum_address(address))

    def get_transactions(self) -> List[Dict[str, Any]]:
        """
        Gets the transactions from Etherscan
        """
        params = self.configuration.parameters

        # required parameters
        address = self.to_checksum(params.get(KEY_ADDRESS))
        api_key = params.get(KEY_API_KEY)

        # optional parameters with default values
        start_block = 0
        end_block = 99999999
        page = 1
        offset = 100
        sort = 'asc'

        if params.get(KEY_START_BLOCK):
            start_block = params.get(KEY_START_BLOCK)
        if params.get(KEY_END_BLOCK):
            end_block = params.get(KEY_END_BLOCK)
        if params.get(KEY_PAGE):
            page = params.get(KEY_PAGE)
        if params.get(KEY_OFFSET):
            offset = params.get(KEY_OFFSET)
        if params.get(KEY_SORT):
            sort_options = ['asc', 'desc']
            if params.get(KEY_SORT) and params.get(KEY_SORT) in sort_options:
                sort = params.get(KEY_SORT)
        
        es = etherscan.Client(
            api_key=api_key
        )

        return es.get_transactions_by_address(
            address=address,
            start_block=start_block,
            end_block=end_block,
            page=page,
            limit=offset,
            sort=sort
        )

    def run(self) -> None:
        """
        Main execution code
        """
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)

        # Create output table (Tabledefinition - just metadata)
        table = self.create_out_table_definition('output.csv', incremental=True, primary_key=['timestamp'])

        # get file path of the table (data/out/tables/Features.csv)
        out_table_path = table.full_path
        logging.info(out_table_path)

        # get our etherscan transactions
        transactions = self.get_transactions()

        # get our field names
        fieldnames = transactions[0].keys() if transactions else []

        # save into out_table_path
        with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transactions)

        # Save table manifest (output.csv.manifest) from the tabledefinition
        self.write_manifest(table)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
