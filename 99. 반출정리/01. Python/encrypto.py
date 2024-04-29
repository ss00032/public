import sys

from Crypto.Cipher import AES
import base64

# 암호화할 데이터
plain_text = "EtlAdmin1!!"

# AES 키 (16바이트, 24바이트 또는 32바이트)
key = "abcdefghijklmnop"

# 암호화 모드 (CBC, ECB 등)
mode = AES.MODE_CBC

# 초기화 벡터 (IV)
iv = "1234567890123456"

# AES 객체 생성
cipher = AES.new(key.encode(), mode, iv.encode())

# 데이터 패딩
padding_length = AES.block_size - len(plain_text) % AES.block_size
padding_text = chr(padding_length) * padding_length
plain_text += padding_text

# 데이터 암호화
encrypted_text = cipher.encrypt(plain_text.encode())

# base64 인코딩
base64_encrypted_text = base64.b64encode(encrypted_text)

# 결과 출력
print("Plain Text : ", plain_text)
print("Encrypted Text : ", base64_encrypted_text.decode())
