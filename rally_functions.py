import boto3
import os
import json
from pyral import Rally
from textwrap import dedent

# Create the Rally Story and add proper tags #
def createRallyStory(payload, logger):
    """
    Creates a Rally user story based on the provided payload and logs the process.
    Args:
        payload (dict): A dictionary containing the following keys:
            - title (str): The title of the user story.
            - goal (str): The goal of the user story.
            - benefit (str): The benefit of the user story.
            - role (str): The role associated with the user story.
            - ac1 (str, optional): Acceptance criteria 1.
            - ac2 (str, optional): Acceptance criteria 2.
            - ac3 (str, optional): Acceptance criteria 3.
            - ac4 (str, optional): Acceptance criteria 4.
            - ac5 (str, optional): Acceptance criteria 5.
            - environment (str): The environment for the user story.
            - rt (str): The release train and team ID in the format "release_train|team_id".
            - iteration (str): The iteration for the user story.
            - time_sensitive (bool): Whether the user story is time-sensitive.
            - contacts (list): A list of dictionaries containing contact information with the key "email".
        logger (Logger): A logger instance to log errors and information.
    Returns:
        dict: A dictionary containing the following keys if the user story is created successfully:
            - link (str): The URL link to the created user story in Rally.
            - id (str): The formatted ID of the created user story.
            - email_list (str): A comma-separated list of email addresses of the contacts.
            - rt_name (str): The name of the release train.
            - rt_team_id (str): The team ID of the release train.
            - sg_api_key (str): The SendGrid API key.
        None: If an error occurs during the creation of the user story.
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

    if os.environ.get('appEnvironment') == 'PROD':
        master_rally_org = f"{secrets['rally_prod_org']}"
        master_rally_api_key = f"{secrets['rally_prod_apikey']}"
    else:
        master_rally_org = f"{secrets['rally_nonprod_org']}"
        master_rally_api_key = f"{secrets['rally_nonprod_apikey']}"

    sendgrid_api_key = f"{secrets['sendgrid_api_key']}"

    if master_rally_api_key == None:
      raise Exception("\n*** ERROR *** master_rally_api_key is not set, you must set this in your code.")

    # Parse all values from the payload #
    title = payload['title']
    goal = payload['goal']
    benefit = payload['benefit']
    role = payload['role']
    ac1 = ("<strong>AC1: </strong>" + payload['ac1'] + "<br />") if payload['ac1'] is not None else ''
    ac2 = ("<strong>AC2: </strong>" + payload['ac2'] + "<br />") if payload['ac2'] is not None else ''
    ac3 = ("<strong>AC3: </strong>" + payload['ac3'] + "<br />") if payload['ac3'] is not None else ''
    ac4 = ("<strong>AC4: </strong>" + payload['ac4'] + "<br />") if payload['ac4'] is not None else ''
    ac5 = ("<strong>AC5: </strong>" + payload['ac5'] + "<br />") if payload['ac5'] is not None else ''
    environment = payload['environment']

    tRT = payload['rt'].split('|')
    release_train = tRT[0]
    release_train_team_id = tRT[1]

    iteration = payload['iteration']
    time_sensitive = payload['time_sensitive']

    # Set Server Variables #
    server = 'rally1.rallydev.com'
    workspace = 'Cox Automotive'
    project = 'C&MS SRE Portfolio Scrum Teams (RT) - C&MS'

    # Setup Rally Client
    rally = Rally(server, apikey=master_rally_api_key, workspace=workspace, project=project)

    # If this is an Emergency set the EXPEDITE flag and prepend the title #
    if time_sensitive == True:
        expedited = True
        title = f'EXPEDITED: {title}'
    else:
        expedited = False
        title = f'{title}'

    # Generate Comma seperated list of email contacts (email addresses only) #
    emails = [p["email"] for p in payload['contacts']]
    emailListString = ','.join(emails)

    # Create the message that will be posted in slack Description #
    displayMessage = dedent(f"""
      Release Train: {release_train} ({release_train_team_id})<br /><br />
      As a(n) {role}, I want to {goal} so that {benefit}.<br /><br />
      <strong>Environment:</strong> {environment}<br />
      <strong>Requested Iteration:</strong> {iteration}<br />
      <strong>Requested Priority:</strong> {"STANDARD" if time_sensitive == False else "EXPEDITED"}<br />
      <strong>Point(s) of Contact (POC):</strong> {emailListString}<br /><br />
      <strong>Acceptance Criteria:</strong><br />
      {ac1}
      {ac2}
      {ac3}
      {ac4}
      {ac5}
    """)

    # Create the list of tags #
    tagList = ["C&MS-SRE", "SLACK_RALLY_AUTOMATION"]

    if payload['title'].startswith('-=TEST=-'):
        tagList.append("CMS-SRE-TEST-STORY")

    # Make sure the tags exist in Rally #
    for newTag in tagList:
      # Define the tag
      tag_data = {
          "Name": f"{newTag}"
      }
      # Create the tag
      tag = rally.create('Tag', tag_data)

    iteration_ref = None
    iteration_data = None
    
    if iteration != "UNSCHEDULED":
        iteration_data = rally.get('Iteration', fetch="Name,StartDate,EndDate,_ref", query="Name = %s" % iteration)
    
    if iteration_data != None and iteration_data.resultCount > 0:
        iteration_ref = iteration_data.data['Results'][0]['_ref']

    # Define the user story data
    story_data = {
        "Project": "/project/719421873493",  # Use your project number
        "Name": title,
        "Description": displayMessage,
        "ScheduleState": "Defined",
        "Expedite": expedited,
        "c_ProductIntakeID": payload['rt']
    }

    if iteration_ref != None:
        story_data['Iteration'] = iteration_ref

    try:
        # Create the user story
        story = rally.create('UserStory', story_data)

        # Get the just added story #
        storyForTagging = rally.get('Story', fetch="FormattedID,Name,Description,Tags", 
                       query="FormattedID = %s" % story.FormattedID,
                       server_ping=False, isolated_workspace=True, instance=True)

        # Get all of the tags in Rally #
        allTags = rally.get('Tag', fetch="true", order="Name", server_ping=False, isolated_workspace=True)
        
        # Define Placeholder for Tag Values from Rally #
        tags = []

        # Populate the tag values
        for tag in allTags:
            if tag.Name in tagList:
                tags.append(tag)

        # Add the tags to the story #
        addTagResponse = rally.addCollectionItems(storyForTagging, tags)

        # Generate response data to send to slack
        response_data = {
            "link": f"https://rally1.rallydev.com/#/?detail=/userstory/{story.ObjectID}&fdp=true",
            "id": story.FormattedID,
            "email_list": emailListString,
            "rt_name": release_train,
            "rt_team_id": release_train_team_id,
            "sg_api_key": sendgrid_api_key
        }

        return response_data

    except Exception as e:
        
        logger.error(e)

        return None