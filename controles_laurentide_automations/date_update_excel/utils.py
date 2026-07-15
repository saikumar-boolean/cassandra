import os
from datetime import datetime, timezone
import json
import msal
import base64
import requests
from dateutil import parser as dateparser
import math
import numpy as np
import pandas as pd
import time

context = None

def set_context(context_obj):
    global context
    context = context_obj

def print_modified(str_to_print_modified):
    global context
    if context is not None:
        context.log.info(str_to_print_modified)
    else:
        print(str_to_print_modified)

def parse_graph_datetime(dt_str):
    """
    Parse Graph API datetime strings to a timezone-aware datetime object.
    Supports both:
      - 2026-01-09T11:59:10Z
      - 2026-01-09T11:59:10+00:00
    """
    if dt_str.endswith("Z"):
        # convert Z to +00:00 for fromisoformat
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)

def adjust_column_width(sheet, workbook, file_path):
    for column_cells in sheet.columns:
        max_length = 0
        column = column_cells[0].column_letter  # Get the column letter (e.g., 'A', 'B', 'C')

        for cell in column_cells:
            try:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
            except TypeError:
                pass

        adjusted_width = (max_length) * 1.2  # Additional padding and scaling factor for better fit
        sheet.column_dimensions[column].width = adjusted_width    
    workbook.save(file_path)
    

def get_graph_token(tenant_id, client_id, client_secret):
    AUTHORITY = f"https://login.microsoftonline.com/{tenant_id}"
    SCOPE = ["https://graph.microsoft.com/.default"]
    app = msal.ConfidentialClientApplication(
        client_id,
        authority=AUTHORITY,
        client_credential=client_secret
    )

    result = app.acquire_token_silent(SCOPE, account=None)

    if not result:
        result = app.acquire_token_for_client(scopes=SCOPE)

    if "access_token" not in result:
        raise Exception(f"Authentication failed: {result}")

    return result["access_token"]

def get_latest_attachment_from_email_graph(
    token,
    mailbox_user_principal_name,
    outlook_folder,
    attachment_keyword="",
    file_type="",
    email_timestamp="",
    script_location=""
):
    headers = {"Authorization": f"Bearer {token}"}

    # ---------- normalize timestamp ----------
    if email_timestamp:
        ts_threshold = parse_graph_datetime(email_timestamp)
    else:
        ts_threshold = datetime.min.replace(tzinfo=timezone.utc)

    # ---------- STEP 1: get root folder id ----------
    root_url = f"https://graph.microsoft.com/v1.0/users/{mailbox_user_principal_name}/mailFolders/msgfolderroot"
    root = requests.get(root_url, headers=headers).json()
    root_id = root["id"]

    # ---------- STEP 2: find target folder ----------
    def find_folder(target_name):
        queue = [root_id]
        visited = set()

        print_modified(f"Target Folder: {target_name}")

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue
            visited.add(current)

            url = (
                f"https://graph.microsoft.com/v1.0/users/{mailbox_user_principal_name}"
                f"/mailFolders/{current}/childFolders?$top=100"
            )
            resp = requests.get(url, headers=headers).json()

            for folder in resp.get("value", []):
                # Debug visibility
                print_modified(f"Seen folder: {folder['displayName']}")

                if folder["displayName"].lower() == target_name.lower():
                    return folder["id"]

                queue.append(folder["id"])

        return None

    target_folder_id = find_folder(outlook_folder)

    if not target_folder_id:
        raise Exception(f"Folder '{outlook_folder}' not found in mailbox {mailbox_user_principal_name}")

    # ---------- STEP 3: get latest 10 messages ----------
    url_messages = (
        f"https://graph.microsoft.com/v1.0/users/{mailbox_user_principal_name}"
        f"/mailFolders/{target_folder_id}/messages"
        f"?$top=10&$orderby=receivedDateTime desc"
    )

    messages = requests.get(url_messages, headers=headers).json().get("value", [])

    # ---------- STEP 4: process messages ----------
    for msg in messages:
        received = dateparser.parse(msg["receivedDateTime"])
        if received <= ts_threshold:
            continue

        # ---------- STEP 5: get attachments ----------
        att_url = (
            f"https://graph.microsoft.com/v1.0/users/{mailbox_user_principal_name}"
            f"/messages/{msg['id']}/attachments"
        )

        attachments = requests.get(att_url, headers=headers).json().get("value", [])

        for att in attachments:
            if att["@odata.type"] != "#microsoft.graph.fileAttachment":
                continue

            name_lower = att["name"].lower()

            if attachment_keyword.lower() in name_lower and file_type.lower() in name_lower:
                # update timestamp hit
                email_timestamp = parse_graph_datetime(msg["receivedDateTime"]).isoformat()                
                
                # ----------- choose save directory -----------
                if script_location and script_location.strip():
                    save_dir = script_location
                else:
                    save_dir = os.path.join(os.environ["USERPROFILE"], "Downloads")

                os.makedirs(save_dir, exist_ok=True)

                file_path = os.path.join(save_dir, att["name"])

                # overwrite if exists
                if os.path.exists(file_path):
                    os.remove(file_path)

                # write binary content
                content = base64.b64decode(att["contentBytes"])
                with open(file_path, "wb") as f:
                    f.write(content)

                print_modified(f"Saved attachment to {file_path}")
                return file_path, email_timestamp

    return "", ""

def send_outlook_email_graph(
    token,
    mailbox_user_principal_name,
    attachment_directory="",
    attachement_name="",
    recipients=None,
    title="",
    body_text="",
):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # ---------- Normalize recipients ----------
    if recipients is None:
        raise ValueError("recipients parameter cannot be None")

    # If a single string is passed, possibly with semicolons
    if isinstance(recipients, str):
        recipients = [
            r.strip()
            for r in recipients.split(";")
            if r.strip()
        ]

    # Safety check
    if not isinstance(recipients, (list, tuple)) or len(recipients) == 0:
        raise ValueError("recipients must be a non-empty string or list of email addresses")

    # ---------- Build recipient objects ----------
    to_recipients = [{"emailAddress": {"address": r}} for r in recipients]

    # ---------- Build message body ----------
    message = {
        "message": {
            "subject": title,
            "body": {
                "contentType": "Text",
                "content": body_text
            },
            "toRecipients": to_recipients,
            "attachments": []
        },
        "saveToSentItems": True
    }

    # ---------- Handle attachment (optional) ----------
    if attachment_directory and attachement_name:
        file_path = os.path.join(attachment_directory, attachement_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Attachment not found: {file_path}")

        with open(file_path, "rb") as f:
            content_bytes = f.read()

        encoded_content = base64.b64encode(content_bytes).decode("utf-8")

        attachment_obj = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": attachement_name,
            "contentBytes": encoded_content
        }

        message["message"]["attachments"].append(attachment_obj)

    # ---------- Send mail via Graph ----------
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox_user_principal_name}/sendMail"

    response = requests.post(url, headers=headers, json=message)

    if response.status_code not in (200, 202):
        raise Exception(f"Failed to send email: {response.status_code} {response.text}")

    print_modified("Email sent successfully via Microsoft Graph!")
    
# Function to open the Outlook window
def open_outlook():
    try:
        # Dispatch the Outlook Application
        outlook = win32.Dispatch("Outlook.Application")
        namespace = outlook.GetNamespace("MAPI")
        inbox = namespace.GetDefaultFolder(6)  # 6 is the Inbox folder
        explorer = inbox.Display()  # Open the Outlook window
        print_modified("Outlook window opened.")
        return outlook  # Return the Outlook object for further use
    except Exception as e:
        raise Exception(str(e))

# Function to close the Outlook window
def close_outlook(outlook):
    try:
        if outlook:
            explorer = getattr(outlook, "ActiveExplorer", None)  # Get the active Explorer window
            #if explorer is not None:
            #    explorer.Close()  # Close the active window
            print_modified("Outlook window closed.")
        else:
            print_modified("Outlook is not open.")
    except Exception as e:
        raise Exception(str(e))

def update_nested_json_value(data, keys_to_update, new_value):
    """
    Recursively update the value of a nested key in a JSON-like structure.
    """
    if len(keys_to_update) == 1:
        data[keys_to_update[0]] = new_value
    else:
        update_nested_json_value(data[keys_to_update[0]], keys_to_update[1:], new_value)

def update_json_value(file_path, keys_to_update, new_value):
    try:
        if new_value == "": return
        # Step 1: Read the JSON file
        with open(file_path, 'r') as file:
            # Step 2: Parse the JSON content
            data = json.load(file)

        # Step 3: Modify the value associated with the nested key
        update_nested_json_value(data, keys_to_update, new_value)

        # Step 4: Write the modified JSON content back to the file
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=2)

        print_modified(f"Updated value for nested key '{'.'.join(keys_to_update)}' in '{file_path}' to '{new_value}'.")
    except FileNotFoundError:
        raise FileNotFoundError(f"File '{file_path}' not found.")
    
from decimal import Decimal, InvalidOperation

def convert_from_money(value):
    if value is None:
        return None

    try:
        cleaned = (
            str(value)
            .strip()
            .replace("$", "")
            .replace(",", "")
        )
        return Decimal(cleaned)
    except InvalidOperation:
        return None

def levenshtein_distance(str1: str, str2: str, percentage: float) -> tuple[bool, int]:
    n = len(str1)
    m = len(str2)

    d = [[0] * (m + 1) for _ in range(n + 1)]

    decision = False

    if n == 0:
        nr = m
    elif m == 0:
        nr = n
    else:
        for i in range(n + 1):
            d[i][0] = i

        for j in range(m + 1):
            d[0][j] = j

        for i in range(1, n + 1):
            for j in range(1, m + 1):
                cost = 0 if str2[j - 1] == str1[i - 1] else 1

                d[i][j] = min(
                    d[i - 1][j] + 1,     
                    d[i][j - 1] + 1,    
                    d[i - 1][j - 1] + cost  
                )

        nr = d[n][m]

    if str1:
        if int((nr * 100) / len(str1)) <= (100 - percentage):
            decision = True

    return decision, nr


def format_to_money(amount, dollar_sign=True):
    try:
        amount = float(amount)
        if dollar_sign: 
            return f"${amount:,.2f}"
        else:
            return f"{amount:,.2f}"
    except (ValueError, TypeError):
        return amount

def send_outlook_email_graph_html(
    token,
    mailbox_user_principal_name,
    recipients=None,
    title="",
    body_html="",
    signature_html="",
    attachment_directory="",
    attachment_name=""
):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # ---------- Normalize recipients ----------
    if recipients is None:
        raise ValueError("recipients parameter cannot be None")

    if isinstance(recipients, str):
        recipients = [
            r.strip()
            for r in recipients.split(";")
            if r.strip()
        ]

    if not isinstance(recipients, (list, tuple)) or len(recipients) == 0:
        raise ValueError("recipients must be a non-empty string or list of email addresses")

    # ---------- Build recipient objects ----------
    to_recipients = [{"emailAddress": {"address": r}} for r in recipients]

    # ---------- Combine body and signature ----------
    full_html = f"{body_html}{signature_html}"

    # ---------- Build message body ----------
    message = {
        "message": {
            "subject": title,
            "body": {
                "contentType": "HTML",  # <--- HTML format
                "content": full_html
            },
            "toRecipients": to_recipients,
            "attachments": []
        },
        "saveToSentItems": True
    }

    # ---------- Handle attachment (optional) ----------
    if attachment_directory and attachment_name:
        file_path = os.path.join(attachment_directory, attachment_name)

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Attachment not found: {file_path}")

        with open(file_path, "rb") as f:
            content_bytes = f.read()

        encoded_content = base64.b64encode(content_bytes).decode("utf-8")

        attachment_obj = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": attachment_name,
            "contentBytes": encoded_content
        }

        message["message"]["attachments"].append(attachment_obj)

    # ---------- Send mail via Graph ----------
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox_user_principal_name}/sendMail"

    response = requests.post(url, headers=headers, json=message)

    if response.status_code not in (200, 202):
        raise Exception(f"Failed to send email: {response.status_code} {response.text}")

    print_modified("Email sent successfully via Microsoft Graph!")


def _safe_cell(x):
    if x is None:
        return None
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    if isinstance(x, (np.floating,)):
        if np.isnan(x) or np.isinf(x):
            return None
        return float(x)
    if pd.isna(x):
        return None
    return x

def update_sheet(sheet_name, start_cell, df, columns, site_id, file_id, headers):
    time.sleep(3)
    subset = df[columns].copy()

    values = [
        [_safe_cell(v) for v in row]
        for row in subset.to_numpy(dtype=object)
    ]

    if not values:
        return

    end_row = len(values) + int(start_cell[1:]) - 1
    end_col = chr(ord(start_cell[0].upper()) + len(columns) - 1)
    addr = f"{start_cell}:{end_col}{end_row}"

    url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        f"/drive/items/{file_id}"
        f"/workbook/worksheets('{sheet_name}')"
        f"/range(address='{addr}')"
    )

    requests.patch(url, headers=headers, json={"values": values}).raise_for_status()

def wait_for_copy_completion(monitor_url, timeout=120):
    start = time.time()

    while time.time() - start < timeout:
        r = requests.get(monitor_url)

        if r.status_code == 202:
            time.sleep(2)
            continue

        if r.status_code in (200, 201):
            data = r.json()
            return data["resourceId"]

        r.raise_for_status()

    raise TimeoutError("Timed out waiting for copy completion")


def send_outlook_email_with_sharepoint_attachment(
    token,
    mailbox_user_principal_name,
    site_id,
    file_id,
    recipients=None,
    title="",
    body_html="",
    signature_html="",
    delete_after_send=True,
    delete_retries=5
):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    if recipients is None:
        raise ValueError("recipients parameter cannot be None")

    if isinstance(recipients, str):
        recipients = [r.strip() for r in recipients.split(";") if r.strip()]

    if not isinstance(recipients, (list, tuple)) or len(recipients) == 0:
        raise ValueError("recipients must be a non-empty string or list")

    to_recipients = [{"emailAddress": {"address": r}} for r in recipients]

    meta_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}"
    meta_resp = requests.get(meta_url, headers=headers)
    meta_resp.raise_for_status()
    file_name = meta_resp.json()["name"]

    content_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/content"
    file_resp = requests.get(
        content_url,
        headers={"Authorization": f"Bearer {token}"}
    )
    file_resp.raise_for_status()

    encoded_content = base64.b64encode(file_resp.content).decode("utf-8")
    full_html = f"{body_html}{signature_html}"

    message = {
        "message": {
            "subject": title,
            "body": {
                "contentType": "HTML",
                "content": full_html
            },
            "toRecipients": to_recipients,
            "attachments": [
                {
                    "@odata.type": "#microsoft.graph.fileAttachment",
                    "name": file_name,
                    "contentBytes": encoded_content
                }
            ]
        },
        "saveToSentItems": True
    }

    send_url = f"https://graph.microsoft.com/v1.0/users/{mailbox_user_principal_name}/sendMail"
    send_resp = requests.post(send_url, headers=headers, json=message)

    if send_resp.status_code not in (200, 202):
        raise Exception(f"Failed to send email: {send_resp.status_code} {send_resp.text}")

    print_modified("Email sent successfully via Microsoft Graph!")

    if not delete_after_send:
        return

    # Small pause can help if the file is still being finalized somewhere
    time.sleep(2)

    delete_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}"

    delete_headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": "bypass-shared-lock"
    }

    for attempt in range(1, delete_retries + 1):
        del_resp = requests.delete(delete_url, headers=delete_headers)

        if del_resp.status_code in (200, 204):
            print_modified("SharePoint temporary file deleted.")
            return

        # Retry on locked/conflict-ish cases
        if del_resp.status_code in (423, 409, 500, 503):
            wait_seconds = min(2 ** attempt, 15)
            print_modified(
                f"Delete attempt {attempt} failed ({del_resp.status_code}). "
                f"Retrying in {wait_seconds}s..."
            )
            time.sleep(wait_seconds)
            continue

        print_modified(f"Warning: failed to delete SharePoint copy: {del_resp.text}")
        return

    print_modified(f"Warning: failed to delete SharePoint copy after retries: {del_resp.text}")

def write_excel_range(
    access_token,
    site_id,
    file_id,
    sheet_name,
    cell_address,
    values
):
    """
    Write values to a specific Excel range in SharePoint via Graph.

    values can be:
        "Hello"
        [["Hello"]]
        [["A", "B"]]
        [[...], [...]]
    """
    time.sleep(3)

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    if not isinstance(values, list):
        values = [[values]]
    elif isinstance(values, list) and not isinstance(values[0], list):
        values = [values]

    cleaned = []
    for row in values:
        cleaned_row = []
        for v in row:
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                cleaned_row.append("")
            elif v is None:
                cleaned_row.append("")
            else:
                cleaned_row.append(v)
        cleaned.append(cleaned_row)

    url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        f"/drive/items/{file_id}"
        f"/workbook/worksheets/{sheet_name}"
        f"/range(address='{cell_address}')"
    )

    body = {"values": cleaned}

    resp = requests.patch(url, headers=headers, json=body)
    resp.raise_for_status()

    print_modified(f"Wrote to {sheet_name}!{cell_address}")

def refresh_pivots_in_sheet(
    graph_token: str,
    site_id: str,
    file_id: str,
    sheet_name: str,
    wait_seconds: int = 10
) -> None:
    """
    Refresh all PivotTables in a worksheet and force recalculation.
    More reliable for Graph Excel automation.
    """

    base_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}/workbook"

    headers = {
        "Authorization": f"Bearer {graph_token}",
        "Content-Type": "application/json"
    }

    session_resp = requests.post(
        f"{base_url}/createSession",
        headers=headers,
        json={"persistChanges": True}
    )
    session_resp.raise_for_status()
    session_id = session_resp.json()["id"]

    session_headers = {
        **headers,
        "workbook-session-id": session_id
    }

    try:
        sheet_resp = requests.get(
            f"{base_url}/worksheets/{sheet_name}",
            headers=session_headers
        )
        sheet_resp.raise_for_status()
        sheet_id = sheet_resp.json()["id"]

        refresh_resp = requests.post(
            f"{base_url}/worksheets/{sheet_id}/pivotTables/refreshAll",
            headers=session_headers
        )
        refresh_resp.raise_for_status()

        calc_resp = requests.post(
            f"{base_url}/application/calculate",
            headers=session_headers,
            json={"calculationType": "Full"}
        )
        calc_resp.raise_for_status()

        time.sleep(wait_seconds)

        print(f"PivotTables refreshed in '{sheet_name}'.")

    finally:
        requests.post(
            f"{base_url}/closeSession",
            headers=session_headers
        )

from decimal import Decimal, ROUND_HALF_UP

def normalize_numeric(value):
    d = Decimal(str(value)).normalize()
    return int(d) if d == d.to_integral_value() else float(d)

def set_excel_column_widths(
    access_token,
    site_id,
    file_id,
    sheet_name,
    columns,
    widths,
    workbook_session_id=None,
    sleep_seconds=3,
    retries=5,
    create_session_if_missing=True,
    close_created_session=True
):
    """
    Set Excel column widths in a SharePoint-hosted workbook via Microsoft Graph.

    columns: e.g. ["A", "B", "AB"]
    widths:  e.g. [120, 80, 200]
    """

    if len(columns) != len(widths):
        raise ValueError("columns and widths must have the same length")

    if not columns:
        return

    if sleep_seconds:
        time.sleep(sleep_seconds)

    base_url = (
        f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        f"/drive/items/{file_id}/workbook"
    )

    base_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    session_id = workbook_session_id
    created_session = False

    # Create a session if one was not provided
    if not session_id and create_session_if_missing:
        session_resp = requests.post(
            f"{base_url}/createSession",
            headers=base_headers,
            json={"persistChanges": True},
            timeout=120
        )
        session_resp.raise_for_status()
        session_id = session_resp.json()["id"]
        created_session = True

    headers = dict(base_headers)
    if session_id:
        headers["workbook-session-id"] = session_id

    last_error_text = None

    try:
        for col, width in zip(columns, widths):
            if not isinstance(col, str) or not col.strip():
                raise ValueError(f"Invalid column value: {col!r}")

            col = col.strip().upper()

            if not isinstance(width, (int, float)) or width < 0:
                raise ValueError(f"Invalid width for column {col}: {width!r}")

            column_range = f"{col}:{col}"

            url = (
                f"{base_url}/worksheets/{sheet_name}"
                f"/range(address='{column_range}')/format"
            )

            body = {"columnWidth": width}

            for attempt in range(1, retries + 1):
                resp = requests.patch(url, headers=headers, json=body, timeout=120)

                if resp.status_code in (200, 201, 204):
                    break

                # Session may have expired; recreate once and retry
                if resp.status_code == 404 and session_id and create_session_if_missing:
                    session_resp = requests.post(
                        f"{base_url}/createSession",
                        headers=base_headers,
                        json={"persistChanges": True},
                        timeout=120
                    )
                    session_resp.raise_for_status()
                    session_id = session_resp.json()["id"]
                    headers["workbook-session-id"] = session_id
                    created_session = True
                    continue

                if resp.status_code in (429, 500, 502, 503, 504):
                    retry_after = resp.headers.get("Retry-After")
                    wait_seconds = (
                        int(retry_after)
                        if retry_after and retry_after.isdigit()
                        else min(2 ** attempt, 20)
                    )
                    last_error_text = resp.text
                    time.sleep(wait_seconds)
                    continue

                resp.raise_for_status()
            else:
                raise requests.HTTPError(
                    f"Failed setting width for {sheet_name}!{column_range}: {last_error_text or 'unknown error'}"
                )

            # Small gap between workbook operations
            time.sleep(0.5)

        print_modified(
            "Set column widths: "
            + ", ".join(f"{c.upper()}={w}" for c, w in zip(columns, widths))
            + f" on {sheet_name}"
        )

    finally:
        if created_session and close_created_session:
            try:
                close_resp = requests.post(
                    f"{base_url}/closeSession",
                    headers=headers,
                    timeout=60
                )
                # closeSession commonly returns 204
                if close_resp.status_code not in (200, 204):
                    print_modified(
                        f"Warning: closeSession returned {close_resp.status_code}: {close_resp.text}"
                    )
            except Exception as e:
                print_modified(f"Warning: failed to close workbook session: {e}")