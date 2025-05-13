# test_pumpportal_ws.py
import json
from websocket import create_connection
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_pumpportal.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

PUMPPORTAL_API_KEY = "85uq0cbcc594rmuedrr62na6e535muhn99qnew216hc2ygthc9mmrdtga1a76gkje9tpuuhfahcpewu2dn36ggat6ru6enuccdj76hukdhh6ana4a9u5acbj9gr5ak9mf16mjavucwyku9mv50h3u89d7eeabagnn6hhtd0ah9pwpanf4u54nvuf0upuj2t8d64cwv3cx0kuf8"
PUMPPORTAL_WS_URL = "wss://pumpportal.fun/api/data"

ws = create_connection(PUMPPORTAL_WS_URL)
logger.info("Connecting to PumpPortal WebSocket...")
ws.send(json.dumps({
    "method": "subPumpFun",
    "data": {"apiKey": PUMPPORTAL_API_KEY}
}))
logger.info("Subscribed to PumpPortal WebSocket")

while True:
    message = ws.recv()
    logger.info(f"Received message: {message}")
