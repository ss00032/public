import sys
import decrypto

# 암호화된 암호
encrypted_password = "xPymsWlRWW4PbkfxpF1JJg=="

# 복호화
decrypted_password = decrypto.decrypt_aes(encrypted_password)
print(decrypted_password)
