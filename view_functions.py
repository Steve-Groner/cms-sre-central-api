from redis_functions import get_all_keys


def getFormView(channel_id):
  """
  Generates a Slack modal view for creating a CMS SRE Rally Backlog Story.

  Args:
    channel_id (str): The ID of the Slack channel where the modal will be used.

  Returns:
    dict: A dictionary representing the Slack modal view.
  """

  # Define the view
  view = {
    "type": "modal",
    "callback_id": "story_data_form",
    "private_metadata":channel_id,
    "title": {
      "type": "plain_text",
      "text": "CMS SRE Story Bot",
      "emoji": True
    },
    "submit": {
      "type": "plain_text",
      "text": "Submit",
      "emoji": True
    },
    "close": {
      "type": "plain_text",
      "text": "Cancel",
      "emoji": True
    },
    "blocks": [
      {
        "type": "header",
        "text": {
          "type": "plain_text",
          "text": "Create CMS SRE Rally Backlog Story",
          "emoji": True
        }
      },
      {
        "type": "section",
        "text": {
          "type": "mrkdwn",
          "text": "Please complete the form below, you will *NOT* be able to edit this form or the story after submission.  Please ensure it is complete and contains all of the necessary information.  If adjustments are needed please reach out to an SRE."
        }
      },
      {
        "type": "divider"
      },
      {
        "block_id": "title_block",
        "type": "input",
        "optional": False,
        "element": {
          "type": "plain_text_input",
          "action_id": "title_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Story Title",
          "emoji": True
        }
      },
      {
        "block_id": "role_block",
        "type": "input",
        "optional": False,
        "element": {
          "type": "static_select",
          "placeholder": {
            "type": "plain_text",
            "text": "Select Requestor Role",
            "emoji": True
          },
          "options": [
            {
              "text": {
                "type": "plain_text",
                "text": "Architect",
                "emoji": True
              },
              "value": "architect"
            },
            {
              "text": {
                "type": "plain_text",
                "text": "AVP",
                "emoji": True
              },
              "value": "avp"
            },
            {
              "text": {
                "type": "plain_text",
                "text": "Director",
                "emoji": True
              },
              "value": "director"
            },
            {
              "text": {
                "type": "plain_text",
                "text": "Manager",
                "emoji": True
              },
              "value": "manager"
            },
            {
              "text": {
                "type": "plain_text",
                "text": "Software Engineer",
                "emoji": True
              },
              "value": "developer"
            },
            {
              "text": {
                "type": "plain_text",
                "text": "SRE",
                "emoji": True
              },
              "value": "sre"
            },
            {
              "text": {
                "type": "plain_text",
                "text": "User",
                "emoji": True
              },
              "value": "user"
            }
          ],
          "action_id": "role_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Requestor Role",
          "emoji": True
        }
      },
      {
        "block_id": "goal_block",
        "type": "input",
        "optional": False,
        "element": {
          "type": "plain_text_input",
          "multiline": True,
          "action_id": "goal_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Story Goal",
          "emoji": True
        }
      },
      {
        "block_id": "benefit_block",
        "type": "input",
        "optional": False,
        "element": {
          "type": "plain_text_input",
          "multiline": True,
          "action_id": "benefit_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Story Benefit/Result/Reason",
          "emoji": True
        }
      },
      {
        "block_id": "ac1_block",
        "type": "input",
        "optional": False,
        "element": {
          "type": "plain_text_input",
          "multiline": False,
          "action_id": "ac1_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Story Acceptance Criteria 1",
          "emoji": True
        }
      },
      {
        "block_id": "ac2_block",
        "type": "input",
        "optional": True,
        "element": {
          "type": "plain_text_input",
          "multiline": False,
          "action_id": "ac2_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Story Acceptance Criteria 2",
          "emoji": True
        }
      },
      {
        "block_id": "ac3_block",
        "type": "input",
        "optional": True,
        "element": {
          "type": "plain_text_input",
          "multiline": False,
          "action_id": "ac3_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Story Acceptance Criteria 3",
          "emoji": True
        }
      },
      {
        "block_id": "ac4_block",
        "type": "input",
        "optional": True,
        "element": {
          "type": "plain_text_input",
          "multiline": False,
          "action_id": "ac4_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Story Acceptance Criteria 4",
          "emoji": True
        }
      },
      {
        "block_id": "ac5_block",
        "type": "input",
        "optional": True,
        "element": {
          "type": "plain_text_input",
          "multiline": False,
          "action_id": "ac5_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Story Acceptance Criteria 5",
          "emoji": True
        }
      },
      {
        "block_id": "env_block",
        "type": "input",
        "optional": False,
        "element": {
          "type": "static_select",
          "placeholder": {
            "type": "plain_text",
            "text": "Select an Environment ...",
            "emoji": True
          },
          "options": [
             {
              "text": {
                "type": "plain_text",
                "text": "Production",
                "emoji": True
              },
              "value": "PRODUCTION"
            },
            {
              "text": {
                "type": "plain_text",
                "text": "Non-Production",
                "emoji": True
              },
              "value": "NONPROD"
            },
          ],
          "action_id": "env_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Environment",
          "emoji": True
        }
      },
      {
        "block_id": "rt_block",
        "type": "input",
        "optional": False,
        "element": {
          "type": "static_select",
          "placeholder": {
            "type": "plain_text",
            "text": "Select a your release train",
            "emoji": True
          },
          "options": (populateReleaseTrainDropdown()),
          "action_id": "release_train_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Release Train",
          "emoji": True
        }
      },
      {
        "block_id": "time_sensitive_block",
        "type": "input",
        "optional": True,
        "element": {
            "type": "checkboxes",
            "options": [
                {
                    "text": {
                        "type": "plain_text",
                        "text": "Time Sensitive",
                        "emoji": True
                    },
                    "value": "TIME_SENSITIVE"
                }
            ],
            "action_id": "time_sensitive_field"
        },
        "label": {
            "type": "plain_text",
            "text": "If this is a time sensitive request check the box below.  All other requests will be prioritized accordingly.",
            "emoji": True
        }
	  },
      {
        "block_id": "iteration_block",
        "type": "input",
        "optional": False,
        "element": {
          "type": "static_select",
          "placeholder": {
            "type": "plain_text",
            "text": "Select an Iteration ...",
            "emoji": True
          },
          "options": (populateIterationDropdown()),
          "action_id": "iteration_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Requested Rally Iteration",
          "emoji": True
        }
      },
      {
        "block_id": "users_block",
        "type": "input",
        "optional": False,
        "element": {
          "type": "multi_users_select",
          "placeholder": {
            "type": "plain_text",
            "text": "Select users",
            "emoji": True
          },
          "action_id": "users_field"
        },
        "label": {
          "type": "plain_text",
          "text": "Who to contact for details",
          "emoji": True
        }
      }
    ]
  }

  return view

def populateReleaseTrainDropdown():
  """
  Retrieves all release trains from a specified namespace and parent key, 
  formats them into a list of dictionaries suitable for a dropdown menu, 
  and sorts the list by the 'value' field.
  Returns:
    list: A sorted list of dictionaries, each containing 'text' and 'value' 
        keys for the dropdown menu.
  """

    #Example get all keys for parent key
  rts = get_all_keys(namespace='cms_sre',parent_key='VW_RELEASE_TRAINS')
  
  #Example get all keys for parent key
  #rts = get_all_keys(namespace=general_namespace,parent_key='VW_RALLY_INTEGRATIONS')

  items = []

  if len(rts) > 0:
     for rt in rts:
        obj = {
          "text": {
            "type": "plain_text",
            "text": f"{rt['RELEASE_TRAIN']}",
            "emoji": True
          },
          "value": f"{rt['RELEASE_TRAIN']}|{rt['TEAM_ID']}"
        }

        items.append(obj)

  sortedData = sorted(items, key=lambda x: x['value'])

  return sortedData

def populateIterationDropdown():
  """
  Retrieves iteration data, appends an "UNSCHEDULED" option, and formats it for a dropdown menu.

  This function fetches all iteration keys from a specified namespace and parent key,
  appends an "UNSCHEDULED" option to the list, and formats each iteration into a dictionary
  suitable for a dropdown menu. The resulting list is sorted in descending order by the iteration name.

  Returns:
    list: A list of dictionaries, each representing an iteration option for a dropdown menu.
  """

  #Example get all keys for parent key
  iters = get_all_keys(namespace='cms_sre',parent_key='VW_RALLY_ITERATIONS')

  iters.append({"NAME":"UNSCHEDULED"})

  items = []

  if len(iters) > 0:
     for it in iters:
        obj = {
          "text": {
            "type": "plain_text",
            "text": f"{it['NAME']}",
            "emoji": True
          },
          "value": f"{it['NAME']}"
        }

        items.append(obj)

  sortedData = sorted(items, key=lambda x: x['value'],reverse=True)

  return sortedData