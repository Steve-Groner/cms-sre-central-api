# Using Sendgrid Send Email Notifications #
from textwrap import dedent
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import *
import os
import boto3
import json

def sendEmailNotification(ticket_data, story_data):
  """
  Sends an email notification using SendGrid API with details from the provided ticket and story data.

  Args:
    ticket_data (dict): A dictionary containing ticket information. Expected keys are:
      - 'title' (str): The title of the ticket.
      - 'goal' (str): The goal of the ticket.
      - 'benefit' (str): The benefit of the ticket.
      - 'time_sensitive' (bool): Indicates if the ticket is time-sensitive.
      - 'environment' (str): The environment related to the ticket.
      - 'iteration' (str): The requested iteration for the ticket.
    story_data (dict): A dictionary containing story information. Expected keys are:
      - 'contacts' (str): The contacts related to the story.
      - 'id' (str): The ID of the story.
      - 'link' (str): The link to the story.
      - 'requester' (str): The requester of the story.
      - 'rt' (str): The release train of the story.
      - 'rt_team_id' (str): The release train team ID of the story.

  Returns:
    None
  """

  # Get Secrets Values #
  os.environ["appEnvironment"] = "PROD"
  client = boto3.client('secretsmanager',region_name='us-east-1')

  secretData = client.get_secret_value(
      SecretId='arn:aws:secretsmanager:us-east-1:473451415060:secret:rally-integration-secrets-aYipd9'
  )

  os.environ["app_secrets"] = secretData['SecretString']

  # Parse Secrets
  secrets = json.loads(os.environ.get('app_secrets'))

  sendgrid_api_key = f"{secrets['sendgrid_api_key']}"

  # Parse the payload #
  title = ticket_data['title']
  goal = ticket_data['goal']
  benefit = ticket_data['benefit']
  time_sensitive = "YES" if ticket_data['time_sensitive'] == True else "NO"
  contacts = story_data['contacts']
  storyid = story_data['id']
  storylink = story_data['link']
  requester = story_data['requester']
  release_train = story_data['rt']
  release_train_team_id = story_data['rt_team_id']
  environment = ticket_data['environment']
  iteration = ticket_data['iteration']

  # Set the email properties #
  subjectContent = "SRE Rally Backlog Story Created"

  htmlmsg = dedent(f"""
    {requester} has submitted an expedited SRE Rally Backlog Story.  Please review the story details below 
    if you are able to help the user with this please assign yourself as the owner of the story and contact
    the customer.<br /><br>

    <strong><h2>Story Details</strong></h2><br />
    <strong>Release Train:</strong> {release_train} ({release_train_team_id})<br />
    <strong>Requester:</strong> {requester}<br />
    <strong>Story ID:</strong> {storyid}<br />
    <strong>Story Title:</strong> {title}<br />
    <strong>Story Link:</strong> <a href='{storylink}' target='_blank'>View in Rally</a><br />
    <strong>Environment:</strong> {environment}<br />
    <strong>Time Sensitive:</strong> {time_sensitive}<br />
    <strong>Requested Iteration:</strong> {iteration}<br />
    <strong>Contact(s):</strong> {contacts}<br /><br />
    <strong>Goal:</strong><br />
    {goal}<br /><br />
    <strong>Benefit:</strong><br />
    {benefit}<br />
  """)

  # Define the message object #
  message = Mail(
      from_email='noreply@coxautoinc.com',
      to_emails=[To('steve.groner@coxautoinc.com'),To('consumermarketingsolutions-sre@coxautoinc.com')],
      subject=subjectContent,
      html_content=htmlmsg,
      is_multiple=True
  )

  # Authenticate with API Key and Send the Message #
  sg = SendGridAPIClient(sendgrid_api_key)
  response = sg.send(message)