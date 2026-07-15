"""Source for Salesforce depending on the simple_salesforce python package.

Imported resources are: account, campaign, contact, lead, opportunity, pricebook_2, pricebook_entry, product_2, user and user_role

Salesforce api docs: https://developer.salesforce.com/docs/apis

To get the security token: https://onlinehelp.coveo.com/en/ces/7.0/administrator/getting_the_security_token_for_your_salesforce_account.htm
"""

from dlt.sources import DltResource
from dlt.sources import incremental

from typing import Iterable

import dlt
from simple_salesforce import Salesforce
from dlt.common.typing import TDataItem


from .helpers import get_records


@dlt.source(name="salesforce")
def salesforce_source(
    table_name: str,
    user_name: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    security_token: str = dlt.secrets.value
) -> Iterable[DltResource]:
    """
    Retrieves data from Salesforce using the Salesforce API.

    Args:
        table_name: Name of the table to retrieve.
        user_name (str): The username for authentication. Defaults to the value in the `dlt.secrets` object.
        password (str): The password for authentication. Defaults to the value in the `dlt.secrets` object.
        security_token (str): The security token for authentication. Defaults to the value in the `dlt.secrets` object.

    Yields:
        DltResource: Data resources from Salesforce.
    """

    client = Salesforce(
        user_name,
        password,
        security_token,
    )

    # define resources
    @dlt.resource(
        write_disposition="replace",
        name=table_name
    )
    def sf_table() -> Iterable[TDataItem]:
        yield get_records(client, table_name)

    return (
        sf_table
    )
