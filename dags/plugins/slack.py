import requests

from airflow.models import Variable


def on_success_callback(context):
    text=f"""
                :green_apple: Task Success.
                *Task*: {context.get('task_instance').task_id}  
                *Dag*: {context.get('task_instance').dag_id}
                *Execution Time*: {context.get('execution_date')}  
                *Log Url*: {context.get('task_instance').log_url}
        """
    
    send_message_to_a_slack_channel(text, ":laughing:")


def on_failure_callback(context):
    text=f"""
                :red_circle: Task Failed.
                *Task*: {context.get('task_instance').task_id}  
                *Dag*: {context.get('task_instance').dag_id}
                *Execution Time*: {context.get('execution_date')}  
                *Log Url*: {context.get('task_instance').log_url}
        """
    
    text += "```" + str(context.get('exception')) +"```"
    send_message_to_a_slack_channel(text, ":scream:")


def send_message_to_a_slack_channel(message, emoji):
    url = "https://hooks.slack.com/services/" + Variable.get("slack_url")
    headers = {
        'content-type': 'application/json',
    }
    data = {"username": "Odigodi", "text": message, "icon_emoji": emoji }
    r = requests.post(url, json=data, headers=headers)
    return r
