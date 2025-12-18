from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest
from kafka import KafkaProducer
import json
import asyncio

api_id = "-"
api_hash = "-"
client = TelegramClient('session', api_id, api_hash)

producer = KafkaProducer(bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'))

producer.send('telegram_messages', {"username":"INIT", "timestamp":"2023-01-01T00:00:00"})
producer.flush()
print("Test message sent!")

channel_ids = ['postupashki_career','dslab','cutterpool','data_secrets','opento_data','dododev']

@client.on(events.NewMessage(chats=channel_ids))
async def handler(event):
    sender = await event.get_sender()
    username = sender.username or str(sender.id)
    msg_data = {"username": username,"timestamp": event.message.date.isoformat()}
    print(f"Message from {username}: {msg_data['timestamp']}")
    producer.send('telegram_messages', msg_data)
    producer.flush()

async def main():
    await client.start()
    for ch_id in channel_ids:
        try:
            entity = await client.get_entity(ch_id)
            await client(JoinChannelRequest(entity))
            print(f"Joined channel: {ch_id}")
        except Exception as e:
            print(f"Error joining channel {ch_id}: {e}")

    await client.run_until_disconnected()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
