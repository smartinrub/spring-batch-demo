package org.smartinrub.springbatchdemo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Credentials {

    private final String id;
    private final String password;
}
