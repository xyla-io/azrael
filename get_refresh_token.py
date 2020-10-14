import webbrowser
import flask
import requests
import getpass

from threading import Timer
from typing import Dict

def generate_access_token(refresh_token: str, client_id: str, client_secret: str):
  payload = {
    'grant_type': 'refresh_token',
    'code': refresh_token, 
    'client_id': client_id,
    'client_secret': client_secret,
  }
  response = requests.post('https://accounts.snapchat.com/login/oauth2/access_token', data=payload)
  response_json = response.json()
  print('Your access token is:\n{access_token}'.format(access_token=response_json['access_token']))

def generate_access_token_and_refresh_token(code: str, client_id: str, client_secret: str) -> Dict[str, any]:
  payload = {
    'grant_type': 'authorization_code',
    'code': code, 
    'client_id': client_id,
    'client_secret': client_secret,
  }
  response = requests.post('https://accounts.snapchat.com/login/oauth2/access_token', data=payload)
  response_json = response.json()
  print('Your access token is:\n{access_token}'.format(access_token=response_json['access_token']))
  print('Your refresh token is:\n{refresh_token}'.format(refresh_token=response_json['refresh_token']))
  return response_json

code = 'CODE'
client_id = input('Please enter your oauth app\'s client ID > ')
client_secret = getpass.getpass('Please enter your oauth app\'s client secret > ')

webbrowser.open('https://accounts.snapchat.com/login/oauth2/authorize?client_id={client_id}&response_type=code&scope=snapchat-marketing-api&redirect_uri=https://127.0.0.1:5100/oauth/snapchat'.format(client_id=client_id))

app = flask.Flask('azrael')

@app.route('/oauth/snapchat', methods=['GET'])
def snapchat():
  code = flask.request.args.get('code')
  print('Your authorization code is:\n{code}'.format(code=code))
  tokens = generate_access_token_and_refresh_token(code=code, client_id=client_id, client_secret=client_secret)
  timer = Timer(0, flask.request.environ.get('werkzeug.server.shutdown'))
  timer.start()
  return flask.jsonify(tokens)

app.run('localhost', 5100, ssl_context='adhoc')
