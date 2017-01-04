#Camel Fast File Consumer

This project contains example `org.apache.camel.component.file.GenericFileExclusiveReadLockStrategy` implementations that speed up file consumption when the only options for determining a read lock are the 'changed' strategy.

##Requirements

- [Apache Maven 3.x](http://maven.apache.org)

##Building Example

Run the Maven build

```
~$ cd $PROJECT_ROOT
~$ mvn clean install
```

##Running Camel

Run the Maven Camel Plugin

```
~$ cd $PROJECT_ROOT
~$ mvn camel:run
```

You can also pass in extra JVM args to bump logging and point to an external config file

```
~$ cd $PROJECT_ROOT
~$ mvn camel:run -Dorg.slf4j.simpleLogger.log.org.apache.camel.component.file=debug -DconfigFile=$HOME/myConfig.properties
```


##Testing

Simply write a file into the configured "input" directory. To really see things work, you'll want to simulate a slow writer. To do so, you can use the following bash command in a separate terminal window:

```
~$ cd $HOME/input #Replace this with your configured "input" directory.
~$ while [ 0 -eq 0 ]; do echo 'foo' >> file01.txt; sleep 0.5; done;
```

Create as many of these "slow writers" as you'd like (using separate terminal windows and unique file names for each) to simulate multiple files being written simultaneously. The files should not be picked up by the Camel routes until you kill the writer script. Once you do, the Camel route will detect that the file has stopped changing and will pick it up.