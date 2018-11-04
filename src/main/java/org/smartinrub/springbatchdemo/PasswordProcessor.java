package org.smartinrub.springbatchdemo;

import lombok.extern.slf4j.Slf4j;

import org.mindrot.jbcrypt.BCrypt;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class PasswordProcessor implements ItemProcessor<Credentials, Credentials> {

    private static final int BCRYPT_ROUNDS = 13;

    @Override
    public Credentials process(Credentials credentials) throws Exception {
        final String hashPassword = hashPassword(credentials.getPassword());
        log.info("hashing password ("  + credentials.getPassword() + ") into (" + hashPassword + ")");
        return new Credentials(credentials.getId(), hashPassword);
    }

    private String hashPassword(String password) {
        return BCrypt.hashpw(password, BCrypt.gensalt(BCRYPT_ROUNDS));
    }
}
