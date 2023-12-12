import java.security.GeneralSecurityException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
class PMPCrypto {
    public static final Logger LOGGER = LoggerFactory.getLogger(PMPCrypto.class);
    
    public static void main(String[] args) throws Exception {
      Encrypt encrypt = new Encrypt();
      LOGGER.info(encrypt.encryptAES256(args[0]));
    }
}
