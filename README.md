# migration_syncher
**A tool to keep a Postgres database in synch with a git repository**

In the migration process of an Oracle database to Postgres, a git repository is maintained that contains the constituent parts of the source code for that database. Multiple copies of the Postgres database may exist. Changes in the source code are to be distributed over all copies of the Postgres database. The purpose of the migration_syncher program is to help keeping the  copies in synch.

So migration_syncher needs to know which database to keep in synch with which git repository. Therefore it uses a properties file, which should be provided in the first command line argument, like:

```
java -jar migration_syncher.jar my_database.properties
```

If the properties file does not exist, then it will be created with default values. Please adjust it to reflect the right database and the right git repository. The comment in the generated properties file should provide sufficient information to correctly fill in the properties.
## First execution
The first time the program runs with correct properties filled in, it will clone the git repository and create a schema in the database.

The program assumes that the database is in synch with the git repository when it runs the first time. So it doesn’t execute anything from the git repository. It just logs the commit id in the last_commit table and leaves.
## Subsequent executions
Every following run of the program performs the following steps:
* Execute a git pull
* Determine which files have changed since the last time the program ran, based on the stored commit id from the last_commit table and the current commit id in the repository
* Execute each changed file in the database.
* Log the execution results in the file_execution_log table
* If an error occurs, then log the file and the error in the failed_file table.
* Overwrite the commit id in the last_commit table with the current commit id from the repository
* Read all the files that are logged in the failed_file table and execute them again.
Rationale: If the database object that is defined in the file depends on another object that was in error, then maybe the problem is solved now.
If the problem is solved, then the file registration will be removed from the failed_file table. Otherwise it will remain in there.
* If the failed_file table is empty, then exit normally. Otherwise exit in error to signal that some file needs attention.

The program can identify files that have been changed (or added) since the last run, but it does not know anything about dependencies between database objects. So the order in which files are executed in the database may be totally wrong. But by executing the failing files multiple times, dependency problems will eventually sort out, so problems may vanish with multiple program executions.

But of course source files may just be wrong as well. These files will remain in the failed_files table until they are overwritten with a correct version in the git repository or until they are removed manually from the failed_files table.
## Requirements
The program needs Java version 11 or higher.

Because it is kind of hard to get the Jgit interface to properly communicate with a git repository in HTTPS, the commandline interface (java.lang.Runtime.exec(String, String[])) will be used to make the native implementation of git do the communication. So git must be installed in the operating system.

Of course the local filesystem must allow the git repository to be cloned and updated. The user must have access (clone and pull) rights on the central git repository. And the database must be accessible with enough user rights granted to create alter or drop all the involved database objects.