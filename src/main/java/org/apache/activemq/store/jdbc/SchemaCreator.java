package org.apache.activemq.store.jdbc;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.IOExceptionSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SchemaCreator {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaCreator.class);

  private boolean inTx;
  protected Statements statements;
  protected DataSource dataSource;
  protected BrokerService brokerService;
  protected Connection connection;
  private PreparedStatement addMessageStatement;
  private PreparedStatement removedMessageStatement;
  private PreparedStatement updateLastAckStatement;


  public static void create(DataSource dataSource, BrokerService brokerService, Statements statements) throws IOException {
    new SchemaCreator(dataSource, brokerService, statements);
  }

  public SchemaCreator(DataSource dataSource, BrokerService brokerService, Statements statements) throws IOException {
    this.dataSource = dataSource;
    this.brokerService = brokerService;
    this.statements = statements;

    try {
      begin();

      try {
        createLockSchema();
      }
      catch(SQLException e) {
        LOG.warn("Cannot create tables due to: " + e);
        log("Failure Details: ", e);
      }
    }
    finally {
      commit();
    }
  }

  public void begin() throws IOException {
    if(inTx) {
      throw new IOException("Already started.");
    }

    inTx = true;
    connection = getConnection();
  }

  public void commit() throws IOException {
    if(!inTx) {
      throw new IOException("Not started.");
    }

    try {
      executeBatch();

      if(!connection.getAutoCommit()) {
        connection.commit();
      }
    }
    catch(SQLException e) {
      log("Commit failed: ", e);
      this.rollback();
      throw IOExceptionSupport.create(e);
    }
    finally {
      inTx = false;
      close();
    }
  }

  public void rollback() throws IOException {
    if(!inTx) {
      throw new IOException("Not started.");
    }

    try {
      if(addMessageStatement != null) {
        addMessageStatement.close();
        addMessageStatement = null;
      }

      if(removedMessageStatement != null) {
        removedMessageStatement.close();
        removedMessageStatement = null;
      }

      if(updateLastAckStatement != null) {
        updateLastAckStatement.close();
        updateLastAckStatement = null;
      }

      connection.rollback();
    }
    catch(SQLException e) {
      log("Rollback failed: ", e);
      throw IOExceptionSupport.create(e);
    }
    finally {
      inTx = false;
      close();
    }
  }

  protected Connection getConnection() throws IOException {
    if(connection == null) {
      try {
        boolean autoCommit = !inTx;
        connection = dataSource.getConnection();

        if(connection.getAutoCommit() != autoCommit) {
          LOG.trace("Setting auto commit to {} on connection {}", autoCommit, connection);
          connection.setAutoCommit(autoCommit);
        }
      }
      catch(SQLException e) {
        log("Could not get JDBC connection: ", e);
        IOException ioe = IOExceptionSupport.create(e);
        brokerService.handleIOException(ioe);
        throw ioe;
      }

      try {
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      }
      catch(Throwable ignored) {
        LOG.debug("Could not set the transaction isolation level on the connection");
      }
    }

    return connection;
  }

  protected void close() throws IOException {
    if(!inTx) {
      try {

        /**
         * we are not in a transaction so should not be committing ??
         * This was previously commented out - but had adverse affects
         * on testing - so it's back!
         *
         */
        try {
          executeBatch();
        }
        finally {
          if(connection != null && !connection.getAutoCommit()) {
            connection.commit();
          }
        }
      }
      catch(SQLException e) {
        log("Error while closing connection: ", e);
        throw IOExceptionSupport.create(e);
      }
      finally {
        try {
          if(connection != null) {
            connection.close();
          }
        }
        catch(Throwable e) {
          LOG.warn("Close failed: " + e.getMessage(), e);
        }
        finally {
          connection = null;
        }
      }
    }
  }

  protected void executeBatch() throws SQLException {
    try {
      executeBatch(addMessageStatement, "Failed add a message");
    }
    finally {
      addMessageStatement = null;
      try {
        executeBatch(removedMessageStatement, "Failed to remove a message");
      }
      finally {
        removedMessageStatement = null;
        try {
          executeBatch(updateLastAckStatement, "Failed to ack a message");
        }
        finally {
          updateLastAckStatement = null;
        }
      }
    }
  }

  protected void executeBatch(PreparedStatement preparedStatement, String message) throws SQLException {
    if(preparedStatement == null) return;

    try {
      for(int code : preparedStatement.executeBatch()) {
        if(code < 0 && code != Statement.SUCCESS_NO_INFO) {
          throw new SQLException(message + ". Response code: " + code);
        }
      }
    }
    finally {
      try {
        preparedStatement.close();
      }
      catch(Throwable ignored) {
      }
    }
  }

  protected void createLockSchema() throws SQLException, IOException {
    Statement statement = null;
    Lock writeLock = new ReentrantReadWriteLock().writeLock();

    try {
      writeLock.lock();
      boolean lockTableExists = doesLockTableExist();
      statement = connection.createStatement();

      for(String createStatement : statements.getCreateSchemaStatements()) {
        // This will fail usually since the tables will be
        // created already.
        try {
          LOG.debug("Executing SQL: " + createStatement);
          statement.execute(createStatement);
        }
        catch(SQLException e) {
          if(lockTableExists) {
            LOG.debug("Could not create JDBC tables; The lock table already exists." + " Failure was: "
                + createStatement + " Message: " + e.getMessage() + " SQLState: " + e.getSQLState()
                + " Vendor code: " + e.getErrorCode());
          }
          else {
            LOG.warn("Could not create JDBC tables; they could already exist." + " Failure was: "
                + createStatement + " Message: " + e.getMessage() + " SQLState: " + e.getSQLState()
                + " Vendor code: " + e.getErrorCode());
            log("Failure details: ", e);
          }
        }
      }

      connection.commit();
    }
    finally {
      writeLock.unlock();
      close(statement);
    }
  }

  protected boolean doesLockTableExist() {
    ResultSet resultSet = null;
    boolean exists = false;

    try {
      resultSet = connection.getMetaData().getTables(null, null, this.statements.getFullLockTableName(), new String[]{"TABLE"});
      exists = resultSet.next();
    }
    catch(Throwable ignore) {
    }
    finally {
      close(resultSet);
    }

    return exists;
  }

  protected void close(Statement statement) {
    try {
      if(statement != null) {
        statement.close();
      }
    }
    catch(Throwable ignored) {
    }
  }

  protected void close(ResultSet resultSet) {
    try {
      if(resultSet != null) {
        resultSet.close();
      }
    }
    catch(Throwable ignored) {
    }
  }

  protected void log(String prefix, SQLException sqlException) {
    String message = prefix + sqlException.getMessage();

    while(sqlException.getNextException() != null) {
      sqlException = sqlException.getNextException();
      message += ", due to: " + sqlException.getMessage();
    }

    LOG.warn(message, sqlException);
  }
}
