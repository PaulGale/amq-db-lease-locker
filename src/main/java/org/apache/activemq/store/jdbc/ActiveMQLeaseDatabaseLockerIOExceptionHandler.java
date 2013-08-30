package org.apache.activemq.store.jdbc;

import org.apache.activemq.broker.LockableServiceSupport;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.DefaultIOExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ActiveMQLeaseDatabaseLockerIOExceptionHandler extends DefaultIOExceptionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ActiveMQLeaseDatabaseLockerIOExceptionHandler.class);

  @Override
  protected boolean hasLockOwnership() throws IOException {
    boolean hasLock = true;

    if(broker.getPersistenceAdapter() instanceof KahaDBPersistenceAdapter) {
      LockableServiceSupport persistenceAdapter = (LockableServiceSupport) broker.getPersistenceAdapter();
      Locker locker = persistenceAdapter.getLocker();

      if(locker != null && locker instanceof ActiveMQLeaseDatabaseLocker) {
        try {
          if(!locker.keepAlive()) {
            hasLock = false;
          }
        }
        catch(IOException ignored) {
        }

        if(!hasLock) {
          throw new IOException("PersistenceAdapter lock no longer valid using: " + locker);
        }
      }
    }

    return hasLock;
  }

  @Override
  public void setIgnoreAllErrors(boolean ignored) {
    logIgnore("ignoreAllErrors", false);
    super.setIgnoreAllErrors(false);
  }

  @Override
  public void setStopStartConnectors(boolean ignored) {
    logIgnore("stopStartConnectors", true);
    super.setStopStartConnectors(true);
  }

  @Override
  public void setIgnoreSQLExceptions(boolean ignored) {
    logIgnore("ignoreSQLExceptions", false);
    super.setIgnoreSQLExceptions(false);
  }

  protected void logIgnore(String property, Object defaultValue) {
    LOG.warn("An attempt to set the property '{}' was ignored. Leaving it as '{}'", property, defaultValue.toString());
  }
}
