from typing import Optional

from google.cloud import iam_admin_v1
from google.cloud.iam_admin_v1 import types


def create_service_account(
    project_id: str, account_id: str, display_name: Optional[str] = None
) -> types.ServiceAccount:
    """
    Creates a service account.

    project_id: ID or number of the Google Cloud project you want to use.
    account_id: ID which will be unique identifier of the service account
    display_name (optional): human-readable name, which will be assigned to the service account
    """

    iam_admin_client = iam_admin_v1.IAMClient()
    request = types.CreateServiceAccountRequest()

    request.account_id = account_id
    request.name = f"projects/{project_id}"

    service_account = types.ServiceAccount()
    service_account.display_name = display_name
    request.service_account = service_account

    account = iam_admin_client.create_service_account(request=request)

    print(f"Created a service account: {account.email}")
    return account


create_service_account(project_id='etl-finance', account_id='my-service-account')