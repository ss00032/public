import sys
import base64
from Crypto.Cipher import AES

def decrypt_aes(encrypted_text, key = "abcdefghijklmnop", mode=AES.MODE_CBC, iv="1234567890123456"):
    cipher = AES.new(key.encode(), mode, iv.encode())
    decoded_encrypted_text = base64.b64decode(encrypted_text)
    decrypted_text = cipher.decrypt(decoded_encrypted_text)
    unpadded_text = decrypted_text[:-decrypted_text[-1]].decode()
    return unpadded_text
