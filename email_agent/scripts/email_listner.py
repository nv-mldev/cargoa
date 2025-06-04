import os
import time
import asyncio
import httpx
from typing import Any, List, Dict, cast

from dotenv import load_dotenv
from azure.identity.aio import ClientSecretCredential
from msgraph_core import BaseGraphRequestAdapter
from msgraph_core.authentication import AzureIdentityAuthenticationProvider
from kiota_abstractions.api_error import APIError


# Load environment variables from .env file
load_dotenv()

MAILBOX_ADDRESS: str = os.getenv("AZURE_MAILBOX_ADDRESS", "")
if not MAILBOX_ADDRESS:
    raise ValueError("AZURE_MAILBOX_ADDRESS environment variable is not set.")

POLL_INTERVAL_STR = os.getenv("POLL_INTERVAL", "60")
try:
    POLL_INTERVAL = int(POLL_INTERVAL_STR)
    if POLL_INTERVAL <= 0:
        raise ValueError("POLL_INTERVAL must be a positive integer.")
except ValueError:
    raise ValueError(
        "POLL_INTERVAL environment variable must be a valid positive integer."
    )


def get_graph_request_adapter() -> BaseGraphRequestAdapter:
    """
    Authenticate to Microsoft Graph via client credentials flow,
    returning a BaseGraphRequestAdapter instance.
    This function uses environment variables to retrieve the necessary
    credentials:
    - AZURE_TENANT_ID
    - AZURE_CLIENT_ID
    - AZURE_CLIENT_SECRET
    These variables should be set in a .env file or in the environment
    where this script is run.
    Raises a ValueError if any of these variables are not set.
    """
    tenant_id = os.getenv("AZURE_TENANT_ID")
    client_id = os.getenv("AZURE_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET")

    if not all([tenant_id, client_id, client_secret]):
        raise ValueError(
            "AZURE_TENANT_ID, AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET "
            "environment variables must be set."
        )

    # Type assertions to satisfy type checkers
    assert tenant_id is not None
    assert client_id is not None
    assert client_secret is not None

    # Use the asynchronous version of ClientSecretCredential
    # for an asyncio application
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    # Define the required scopes for Graph API access
    # .default scope is common for client credentials flow
    auth_provider = AzureIdentityAuthenticationProvider(
        credential, scopes=["https://graph.microsoft.com/.default"]
    )
    adapter = BaseGraphRequestAdapter(auth_provider)
    return adapter


async def fetch_unread_messages(
    adapter: BaseGraphRequestAdapter, mailbox: str
) -> List[Dict[str, Any]]:
    """
    Query Microsoft Graph for all unread messages in the specified mailbox
    using the Microsoft Graph API.

    :param adapter: An authenticated BaseGraphRequestAdapter instance.
    :param mailbox: The email address of the mailbox to query.
    :return: A list of dictionaries containing message details.
    :raises ValueError: If the mailbox address is not set or if adapter
        missing.
    :raises APIError: If the request to the Microsoft Graph API fails.
    """
    if not adapter:
        raise ValueError("Graph request adapter is not provided or not authenticated.")
    if not mailbox:
        raise ValueError("Mailbox address is not set.")

    # Get the access token from the adapter's auth provider
    auth_provider = getattr(adapter, "_authentication_provider", None)
    if not auth_provider:
        raise ValueError("Could not get authentication provider from adapter")

    access_token_result = await auth_provider.get_authorization_token("")
    if not access_token_result:
        raise ValueError("Could not get access token")

    access_token = access_token_result.token

    # Use httpx to make the request directly
    url = (
        f"https://graph.microsoft.com/v1.0/users/{mailbox}/"
        f"mailFolders/Inbox/messages"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    params = {
        "$filter": "isRead eq false",
        "$select": ("id,subject,receivedDateTime,from,bodyPreview," "conversationId"),
        "$top": "25",
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers, params=params)
        response.raise_for_status()
        response_data = response.json()

    if isinstance(response_data, dict) and "value" in response_data:
        messages = response_data["value"]
        if isinstance(messages, list):
            return cast(List[Dict[str, Any]], messages)
        else:
            print(f"Expected 'value' to be a list, but got {type(messages)}.")
            return []
    else:
        print("Failed to fetch unread messages or 'value' key not found.")
        return []


async def mark_message_as_read(
    adapter: BaseGraphRequestAdapter, message_id: str, mailbox: str
) -> None:
    """Mark a message as read in the specified mailbox by sending a PATCH
    request to the Microsoft Graph API.

    :param adapter: An authenticated BaseGraphRequestAdapter instance.
    :param message_id: The unique identifier of the message to mark as read.
    :param mailbox: The email address of the mailbox containing the message.
    :raises ValueError: If parameters are invalid.
    :raises APIError: If the request to the Microsoft Graph API fails.
    """
    if not adapter:
        raise ValueError("Graph request adapter is not provided or not authenticated.")
    if not message_id:
        raise ValueError("Message ID is not provided.")
    if not mailbox:
        raise ValueError("Mailbox address is not set.")

    # Get the access token from the adapter's auth provider
    auth_provider = getattr(adapter, "_authentication_provider", None)
    if not auth_provider:
        raise ValueError("Could not get authentication provider from adapter")

    access_token_result = await auth_provider.get_authorization_token("")
    if not access_token_result:
        raise ValueError("Could not get access token")

    access_token = access_token_result.token

    # Use httpx to make the PATCH request directly
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/" f"messages/{message_id}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    body = {"isRead": True}

    async with httpx.AsyncClient() as client:
        response = await client.patch(url, headers=headers, json=body)
        response.raise_for_status()

    print(f"Successfully marked message {message_id} as read.")


async def main():
    """
    1. Build a GraphRequestAdapter instance.
    2. In an infinite loop that runs every POLL_INTERVAL seconds:
        a. Fetch unread messages from the mailbox.
        b. For each unread message, print its details and mark it as read.
        c. Sleep for POLL_INTERVAL seconds before the next iteration.
    3. If an error occurs, print the error message and continue after a
       short delay.
    """
    print("Initializing Microsoft Graph adapter...")
    try:
        adapter = get_graph_request_adapter()
        print(
            f"Adapter initialized. Starting poll loop for mailbox: "
            f"{MAILBOX_ADDRESS}"
        )
    except ValueError as e:
        print(f"Configuration error: {e}")
        return  # Exit if basic configuration is wrong

    while True:
        try:
            print(
                f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                f"Fetching unread messages..."
            )
            unread_messages = await fetch_unread_messages(
                adapter, mailbox=MAILBOX_ADDRESS
            )

            if not unread_messages:
                print("No unread messages found.")
            else:
                print(f"Found {len(unread_messages)} unread message(s):")
                for message in unread_messages:
                    subject = message.get("subject", "No Subject")
                    received_date_time = message.get("receivedDateTime", "N/A")
                    sender_info = (
                        message.get("from", {})
                        .get("emailAddress", {})
                        .get("address", "N/A")
                    )
                    body_preview = message.get("bodyPreview", "N/A")
                    message_id = message.get("id")

                    print(f"  Subject: {subject}")
                    print(f"  From: {sender_info}")
                    print(f"  Received: {received_date_time}")
                    print(f"  Preview: {body_preview}")
                    print(f"  ID: {message_id}")

                    if message_id:
                        await mark_message_as_read(adapter, message_id, MAILBOX_ADDRESS)
                    else:
                        print("  Could not mark message as read: ID missing.")
                    print("-" * 30)

            print(f"Waiting for {POLL_INTERVAL} seconds before next poll...")
            await asyncio.sleep(POLL_INTERVAL)

        except APIError as e:
            print(f"Microsoft Graph API Error: {e}")
            # You might want to inspect e.response_status_code or
            # e.response_headers
            # For specific error codes (like 401/403), you might need to
            # re-authenticate or check permissions.
            print("An API error occurred. Waiting for 60 seconds before " "retrying...")
            await asyncio.sleep(60)
        except (
            ValueError
        ) as e:  # Catch configuration or validation errors from our functions
            print(f"Value Error: {e}")
            print("Waiting for 60 seconds before retrying...")
            await asyncio.sleep(60)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            print(f"Type of error: {type(e)}")
            print("Waiting for 60 seconds before retrying...")
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Exiting...")
