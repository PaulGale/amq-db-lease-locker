package org.apache.activemq.store.jdbc;

import javax.sql.DataSource;

public interface LockDataSourceCapable {
  public void setLockDataSource(DataSource lockDataSource);
  public DataSource getLockDataSource();
}
