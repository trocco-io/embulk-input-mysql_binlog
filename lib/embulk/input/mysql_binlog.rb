Embulk::JavaPlugin.register_input(
  "mysql_binlog", "org.embulk.input.mysql_binlog.MysqlBinlogInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
