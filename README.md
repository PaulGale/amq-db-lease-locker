amq-db-lease-locker
===================

An ActiveMQ database backed shared lease locker for use without a JDBC persistence adapter.

## How to use the lease database locker

Note that the locker is included via it's own XSD namespace, namely `http://example.com/activemq/schema/core/example.xsd`.
 The locker tag itself must therefore be prefixed with the chosen namespace stem, `locker:` in this case. The XSD namespace
 can be changed from `example.com` by editing the pom file as appropriate.

Note that the value of the `broker`'s `id` attribute should be assigned to the `activemq-lease-database-locker`'s
`brokerService` attribute.


```xml
<beans
    xmlns="http://www.springframework.org/schema/beans"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:locker="http://example.com/activemq/schema/core"
    xsi:schemaLocation="http://www.springframework.org/schema/beans
                        http://www.springframework.org/schema/beans/spring-beans.xsd
                        http://activemq.apache.org/schema/core
                        http://activemq.apache.org/schema/core/activemq-core.xsd
                        http://example.com/activemq/schema/core
                        http://example.com/activemq/schema/core/example.xsd">

  <broker id="#theBroker">

    <persistenceAdapter>
      <kahaDB directory="${activemq.data}/kahadb"
              lockKeepAlivePeriod="2500"
              useLock="true">
        <locker>
          <locker:activemq-lease-database-locker
                  failIfLocked="false"
                  maxAllowableDiffFromDBTime="1000"
                  brokerService="#theBroker"
                  lockDataSource="#mysql-ds"
                  createLockTableOnStartup="true"
                  lockAcquireSleepInterval="5000"
                  lockTableName="activemq_lock">
          </locker:activemq-lease-database-locker>
        </locker>
      </kahaDB>
    </persistenceAdapter>

  </broker>
```

## Configuring the MySQL (in this case) datasource

A typical MySQL datasource configuration should look something like:

```xml
  <bean id="mysql-ds"
        class="org.apache.commons.dbcp.BasicDataSource"
        destroy-method="close">
    <property name="driverClassName" value="com.mysql.jdbc.Driver"/>
    <property name="url" value="jdbc:mysql://example.com/activemq?autoReconnect=true&amp;failOverReadOnly=false&amp;maxReconnects=1&amp;relaxAutoCommmit=true&amp;useJDBCCompliantTimezoneShift=true"/>
    <property name="username" value="${mysql.username}"/>
    <property name="password" value="${mysql.password}"/>
    <property name="testOnBorrow" value="true"/>
    <property name="validationQuery" value="select 1"/>
    <property name="poolPreparedStatements" value="true"/>
  </bean>
```

Note the use of the attribute `useJDBCCompliantTimezoneShift=true` on the url query string. If you don't use this then
depending on how things are configured the locker's `maxAllowableDiffFromDBTime` will always be exceeded. For example
the broker is configured to run on UTC time but the JDBC result sets have timestamps that reflect the local time zone,
EST in my case. Therefore the locker would complain that the time difference between it and the database was over 4 hours.
Bad things would follow.

## Lease locker exception handler

A lease locker specific exception handler is also provided.

```xml
  <bean id="leaseLockerIOExceptionHandler"
        class="org.apache.activemq.store.jdbc.ActiveMQLeaseDatabaseLockerIOExceptionHandler"/>
```

>The lease locker exception handler prevents certain properties from being overridden.
>A message is logged if an attempt to modify them is detected.

### Locked exception handler properties

[Property descriptions can be found here.](http://activemq.apache.org/configurable-ioexception-handling.html "IOException handler property descriptions")

| Property                 | Locked Value  |
| ------------------------ | ------------- |
| `ignoreAllErrors`        | `false`       |
| `stopStartConnectors`    | `true`        |
| `ignoreSQLExceptions`    | `false`       |

### Regular exception handler properties

| Property                 | Default Value |
| ------------------------ | ------------- |
| `ignoreNoSpaceErrors`    |  `true`       |
| `noSpaceMessage`         |  `space`      |
| `sqlExceptionMessage`    |  `""`         |
| `resumeCheckSleepPeriod` |  `5sec`       |


## Wiring up the lease locker exception handler to the broker

### Broker configuration

The `broker` tag has an attribute called `ioExceptionHandler` which should be assigned the id of the exception handler
bean. Note that the id has an `#` prefix as it's an `xbean` id.

```xml
<beans
    xmlns="http://www.springframework.org/schema/beans"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:l="http://example.com/activemq/schema/core"
    xsi:schemaLocation="http://www.springframework.org/schema/beans
                        http://www.springframework.org/schema/beans/spring-beans.xsd
                        http://activemq.apache.org/schema/core
                        http://activemq.apache.org/schema/core/activemq-core.xsd
                        http://example.com/activemq/schema/core
                        http://example.com/activemq/schema/core/example.xsd">

  <broker ioExceptionHandler="#leaseLockerIOExceptionHandler">

    <bean id="leaseLockerIOExceptionHandler"
          class="org.apache.activemq.store.jdbc.ActiveMQLeaseDatabaseLockerIOExceptionHandler"/>

  </broker>
```

### Modifying the broker's classpath

For ActiveMQ to find the lease locker its jar file should be put on ActiveMQ's classpath. For example it could be
placed in either `%ACTIVEMQ_HOME%/lib/optional` or you could use the `--extdir` command-line switch to specify the
path to the jar file.
