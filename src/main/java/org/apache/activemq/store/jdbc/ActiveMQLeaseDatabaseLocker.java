package org.apache.activemq.store.jdbc;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerServiceAware;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Represents an exclusive lease on a database to avoid multiple brokers running
 * against the same logical database.
 *
 * @org.apache.xbean.XBean element="activemq-lease-database-locker"
 *
 */
public class ActiveMQLeaseDatabaseLocker extends LeaseDatabaseLocker implements BrokerServiceAware, LockDataSourceCapable {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveMQLeaseDatabaseLocker.class);
  protected BrokerService brokerService;
  private boolean createLockTableOnStartup;
  private String lockTableName;

  @Override
  public void configure(PersistenceAdapter ignored) throws IOException {
    statements = getStatements();

    if(createLockTableOnStartup) {
      SchemaCreator.create(dataSource, brokerService, statements);
    }
  }

  @Override
  public boolean keepAlive() throws IOException {
    boolean result = false;
    final String sql = statements.getLeaseUpdateStatement();

    LOG.debug(getLeaseHolderId() + ", lease keepAlive Query is " + sql);

    Connection connection = null;
    PreparedStatement statement = null;

    try {
      connection = dataSource.getConnection();

      initTimeDiff(connection);
      statement = connection.prepareStatement(sql);
      setQueryTimeout(statement);

      final long now = System.currentTimeMillis() + diffFromCurrentTime;
      statement.setString(1, getLeaseHolderId());
      statement.setLong(2, now + lockAcquireSleepInterval);
      statement.setString(3, getLeaseHolderId());

      result = (statement.executeUpdate() == 1);
    }
    catch(Exception e) {
      LOG.warn(getLeaseHolderId() + ", failed to update lease: " + e, e);
      IOException ioe = IOExceptionSupport.create(e);
      brokerService.handleIOException(ioe);
      throw ioe;
    }
    finally {
      close(statement);
      close(connection);
    }

    return result;
  }

  /**
   * Sets whether or not the lock table should be created on startup.
   * @org.apache.xbean.Property propertyEditor="org.apache.activemq.util.BooleanEditor"
   */
  public void setCreateLockTableOnStartup(boolean createTablesOnStartup) {
    this.createLockTableOnStartup = createTablesOnStartup;
  }

  @Override
  public void setBrokerService(BrokerService brokerService) {
    this.brokerService = brokerService;
  }

  @Override
  public String getLeaseHolderId() {
    if(leaseHolderId == null && brokerService != null) {
      leaseHolderId = brokerService.getBrokerName();
    }

    return leaseHolderId;
  }

  @Override
  public void setLockDataSource(DataSource lockDataSource) {
    this.dataSource = lockDataSource;
  }

  @Override
  public DataSource getLockDataSource() {
    return this.dataSource;
  }

  protected Statements getStatements() {
    if(statements == null) {
      statements = new Statements();

      if(lockTableName != null) {
        statements.setLockTableName(lockTableName);
      }

      statements.setCreateSchemaStatements(new String[]{
          "CREATE TABLE " + statements.getFullLockTableName()
              + "(ID " + statements.getLongDataType() + " NOT NULL, TIME " + statements.getLongDataType()
              + ", BROKER_NAME " + statements.getStringIdDataType() + ", PRIMARY KEY (ID))",
          "INSERT INTO " + statements.getFullLockTableName() + "(ID) VALUES (1)",
      });
    }

    return statements;
  }

  public void setStatements(Statements statements) {
    this.statements = statements;
  }

  public void setLockTableName(String lockTableName) {
    this.lockTableName = lockTableName;
  }

  private void setQueryTimeout(PreparedStatement statement) throws SQLException {
    if(queryTimeout > 0) {
      statement.setQueryTimeout(queryTimeout);
    }
  }

  private void close(Connection connection) {
    if(null == connection) return;

    try {
      connection.close();
    }
    catch(SQLException e1) {
      LOG.debug(getLeaseHolderId() + " caught exception while closing connection: " + e1, e1);
    }
  }

  private void close(PreparedStatement statement) {
    if(null == statement) return;

    try {
      statement.close();
    }
    catch(SQLException e1) {
      LOG.debug(getLeaseHolderId() + ", caught while closing statement: " + e1, e1);
    }
  }
}
