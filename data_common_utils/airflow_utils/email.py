import os
import tempfile
import boto3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# Short term hack - need to updated clients
DEFAULT_SOURCE_ARN = 'AASDFASDF'

def create_multipart_message(
        sender: str, recipients: list, title: str, text: str=None, html: str=None, attachments: list=None)\
        -> MIMEMultipart:
    """
    Creates a MIME multipart message object.
    Uses only the Python `email` standard library.
    Emails, both sender and recipients, can be just the email string or have the format 'The Name <the_email@host.com>'.

    :param sender: The sender.
    :param recipients: List of recipients. Needs to be a list, even if only one recipient.
    :param title: The title of the email.
    :param text: The text version of the email body (optional).
    :param html: The html version of the email body (optional).
    :param attachments: List of files to attach in the email.
    :return: A `MIMEMultipart` to be used to send the email.
    """
    multipart_content_subtype = 'alternative' if text and html else 'mixed'
    msg = MIMEMultipart(multipart_content_subtype)
    msg['Subject'] = title
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    # Record the MIME types of both parts - text/plain and text/html.
    # According to RFC 2046, the last part of a multipart message, in this case the HTML message, is best and preferred.
    if text:
        part = MIMEText(text, 'plain')
        msg.attach(part)
    if html:
        part = MIMEText(html, 'html')
        msg.attach(part)

    # Add attachments
    for attachment in attachments or []:
        with open(attachment, 'rb') as f:
            part = MIMEApplication(f.read())
            part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(attachment))
            msg.attach(part)

    return msg

def send_mail(
        sender: str, recipients: list, title: str, text: str=None, html: str=None, attachments: list=None, source_arn=DEFAULT_SOURCE_ARN) -> dict:
    """
    Send email to recipients. Sends one mail to all recipients.
    The sender needs to be a verified email in SES.
    """
    msg = create_multipart_message(sender, recipients, title, text, html, attachments)
    ses_client = boto3.client('sesv2')  # Use your settings here
    return ses_client.send_email(
        # Source=sender,
        # Destinations=recipients,
        # RawMessage={'Data': msg.as_string()}
        FromEmailAddress=sender,
        FromEmailAddressIdentityArn=source_arn,
        Destination={
            'ToAddresses': recipients
        },
        Content={
            'Raw': {
                'Data': msg.as_string()
            }
        }
    )
    

def send_mail_s3_attachment(
        sender: str, recipients: list, title: str, text: str=None, html: str=None, s3_bucket: str=None, s3_keys: list=[],  
        source_arn=DEFAULT_SOURCE_ARN):
    """
    Send email via SES attaching s3 file
    """
    s3 = boto3.client('s3')
    attachement_list =[]
    with tempfile.TemporaryDirectory() as temp_dir:
        for s3_key in s3_keys:
            s3_file = s3_key.split('/')[-1]
            temp_file_path = os.path.join(temp_dir, s3_file)
            with open(temp_file_path, "w+b") as f:
                print(s3_bucket, s3_key)
                file_check = s3.list_objects(Bucket = s3_bucket, Prefix = s3_key)
                if len(file_check.get('Contents', [])) != 0:
                    s3.download_fileobj(s3_bucket, s3_key, f)
                    print(f.name)
                    attachement_list.append(temp_file_path)
        if len(attachement_list) == 0:
            no_data_prefix = 'Note: No data is available for todays feed, if you feel this is in error please contact us...... '
            text = f'{no_data_prefix}{text}'
            html = f'{no_data_prefix}{html}'
        response = send_mail(sender, recipients, title, text, html, attachement_list)
    print(response)
