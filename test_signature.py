import hashlib
import hmac
import base64

# Test values from the latest kraken_balance.py output
path = "/0/private/Balance"
nonce = "1741527507531"  # Replace with the nonce from your latest output
post_data = "nonce=1741527507531"  # Replace with the Post data string from your latest output
secret = "pnevJ3F2UXfzNsutN9bB5oi7Hs6Mb5LBHBWhOmg+47HHb9TP+FL4T87C64ZApfdFk50Iz9U72/H4e4nBUZJbtw=="  # Replace with your KRAKEN_API_SECRET from .env

# Generate path hash
path_hash = hashlib.sha256(path.encode('utf-8')).digest()
print("Path hash (hex):", path_hash.hex())

# Create message
message = path_hash + nonce.encode('utf-8') + post_data.encode('utf-8')
print("Signature message (hex):", message.hex())

# Decode secret and generate signature
secret_bytes = base64.b64decode(secret)
signature = hmac.new(secret_bytes, message, hashlib.sha512).digest()
signature_b64 = base64.b64encode(signature).decode('utf-8')
print("Generated signature (base64):", signature_b64)
