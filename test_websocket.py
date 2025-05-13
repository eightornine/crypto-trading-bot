import asyncio
import websockets
import json
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('websocket_test.log')
    ]
)
logger = logging.getLogger(__name__)

async def test_websocket():
    uri = "wss://pumpportal.fun/api/data"
    subscription_attempts = [
        {"type": "subscribe", "channel": "new_tokens"},
        {"action": "subscribe", "channel": "new_tokens"},
        {"type": "subscribe", "event": "token_creation"},
        {"method": "subscribe", "params": {"channel": "new_tokens"}},
        {"type": "subscribe", "channel": "token_creation"}
    ]

    while True:
        try:
            async with websockets.connect(uri) as websocket:
                logger.info("Connected to PumpPortal WebSocket")

                # Try each subscription message
                for attempt in subscription_attempts:
                    subscription_message = json.dumps(attempt)
                    await websocket.send(subscription_message)
                    logger.info(f"Sent subscription message: {subscription_message}")
                    # Wait a short time to receive a response
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                        logger.info(f"Received message: {message}")
                        try:
                            data = json.loads(message)
                            logger.info(f"Parsed data: {data}")
                            if "errors" not in data:
                                logger.info("Subscription successful! Listening for token events...")
                                break  # Exit the subscription loop if successful
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse message as JSON: {e}")
                    except asyncio.TimeoutError:
                        logger.warning(f"No response received for subscription: {subscription_message}")
                else:
                    logger.error("All subscription attempts failed. Listening for messages anyway...")

                # Listen for messages
                while True:
                    try:
                        message = await websocket.recv()
                        logger.info(f"Received message: {message}")
                        try:
                            data = json.loads(message)
                            logger.info(f"Parsed data: {data}")
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse message as JSON: {e}")
                    except websockets.exceptions.ConnectionClosed as e:
                        logger.error(f"WebSocket connection closed: {e}")
                        break
        except Exception as e:
            logger.error(f"WebSocket error: {str(e)}")
            logger.info("Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(test_websocket())
