import telegram_send

def run_test():
    telegram_connection_established = False
    try:
        telegram_send.send(messages=["startup test message"])
        telegram_connection_established = True
    except Exception as e:
        print("error in sending telegram message:")
        print(e)
    return telegram_connection_established
