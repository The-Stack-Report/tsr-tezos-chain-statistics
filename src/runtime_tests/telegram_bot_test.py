import telegram_send
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

machine = os.getenv("MACHINE")

def run_test():
    telegram_connection_established = False
    try:
        telegram_send.send(messages=[f"|{machine}| startup test message"])
        telegram_connection_established = True
    except Exception as e:
        print("error in sending telegram message:")
        print(e)
    return telegram_connection_established
