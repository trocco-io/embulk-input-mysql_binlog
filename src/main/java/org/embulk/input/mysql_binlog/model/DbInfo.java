package org.embulk.input.mysql_binlog.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DbInfo {
    String host;
    int port;
    String DbName;
    String User;
    String Password;
}
