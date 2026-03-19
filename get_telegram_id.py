import os
import requests
from dotenv import load_dotenv

def get_chat_id():
    load_dotenv()
    token = os.getenv("TELEGRAM_TOKEN_BONGUS")
    
    if not token:
        print("Please set TELEGRAM_TOKEN_BONGUS in your .env file first.")
        return

    url = f"https://api.telegram.org/bot{token}/getUpdates"
    
    try:
        # verify=False is added to bypass the SSL certificate issue on your local Windows machine
        response = requests.get(url, verify=False)
        data = response.json()
        
        if not data.get("ok"):
            print(f"Error from Telegram API: {data}")
            return
            
        updates = data.get("result", [])
        if not updates:
            print("No messages found! Please send a message (like 'hello') to your bot in Telegram right now, then run this script again.")
            return
            
        # Get the chat ID from the most recent message
        latest_update = updates[-1]
        
        if "message" in latest_update:
            chat_id = latest_update["message"]["chat"]["id"]
            username = latest_update["message"]["from"].get("username", "Unknown")
            print(f"✅ Success! Found recent message from @{username}")
            print(f"👉 Your Chat ID is: {chat_id}")
            print(f"Copy this value to TELEGRAM_CHAT_ID_BONGUS in your .env file.")
        else:
            print("Couldn't find a standard message in the latest update. Try sending a normal text message to the bot.")
            
    except Exception as e:
        print(f"Failed to connect to Telegram API: {e}")

if __name__ == "__main__":
    get_chat_id()
