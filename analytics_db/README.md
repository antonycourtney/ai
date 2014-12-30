# Analytic Inbox:  Redshift

## Setup

### Create a RedShift Instance

Once you've created an AWS Account, open the [AWS Console page](https://console.aws.amazon.com/console/home?region=us-west-2) and select "Redshift" (under "Database") to open the Redshift Console.

Click on the button labeled either "Create Cluster" or "Launch Cluster".

In the form, just accept most of the defaults. I filled in only the following:

    Cluster Identifier: my-redshift
    Db name: mydb
    Master User Name: awsuser
    Master user password:   (I generated a fresh one in 1Password and stored this in a new 1Password login under "amazon-aws-redshift")
  
For the node configuration, also accepted defaults, starting with a Single Node configuration (7 EC2 compute units) per node.

### Download an ODBC SQL IDE Client

Redshift uses Postgres SQL, so any SQL IDE that can speak Postgres should work.
So far I've found pgCommander to be a reasonable SQL editor / viewer on the Mac: [https://eggerapps.at/pgcommander/](https://eggerapps.at/pgcommander/)

### Tweak Security Settings

When I initially tried to connect to my RedShift instance from my desktop client, I could not establish an ODBC connection. Some [digging](http://docs.aws.amazon.com/redshift/latest/mgmt/connecting-refusal-failure-issues.html) revealed this is because of security settings, and I needed to explicit set up a security group that would allow connections from external IP addresses.

I tried to follow the instructions on [http://docs.aws.amazon.com/redshift/latest/mgmt/managing-security-groups-console.html](ttp://docs.aws.amazon.com/redshift/latest/mgmt/managing-security-groups-console.html) but they seem to be out of date with UI, since CIDR/IP doesn't seem to be an option on Inbound security rules...

From "Security Groups" page I did:

  * Create Security Group
  * Then added an Inbound rule to allow all traffic for all IPs.  (A more restrictive rule would obviously be better for production, but we'll still have user/password authentication over an SSL wrapped ODBC connection)
  * From Redshift Console, set the VPC Security Group for the Redshift cluster to the newly created security group.

### Set up ODBC Connection

Use the Cluster Database Properties on the "Configuration" tab of the cluster from the AWS Redshift Console to fill in ODBC connection properties in the desktop client app.  Click a button to test the connection and make sure everything works.

### Create Message and Recipients Tables

In your RedShift SQL client, execute the commands in [createTables.sql](analytics_db/createTables.sql)

### Test your JavaScript Postgres Connection

Run the following:

  $ node pgtest.js

If all goes well, you should see the single result '93', and the process should wait for additional input.

## Building and Executing the Inbox Queries

A fundamental problem with SQL is that lacks the basic mechanisms - procedures, functions and variables - that enable parameterization and code reuse in conventional programming languages.  
To work around this limitation, we do not write SQL code directly.  Instead, we use the *template strings* feature of ES6 to
compose SQL queries.  This enables us to use the abstraction mechanisms in JavaScript to achieve basic abstraction and reuse.

### Build the Inbox Queries

Run the following:

  $ gulp

This should print a few lines of diagnostic output, and create a `build/js` directory that will be populated with the result of running the contents of the `queries` directory through the Traceur compiler.

### Test the Inbox Queries

Run:

  $ node query_test.js rawMessageCount

If all goes well you should see a large amount of SQL output followed by results for two queries.  The first query simply counts the number of messages in the `messages` table; the second query obtains the top correspondents for the current user.

### Rebuild the Derived Tables

Analytic Inbox is oriented around the concept of a *correspondent* - an individual with a unique real name and a set of email addresses.

We materialize a few tables on RedShift for maintaining tables of correspondents and their email addresses, and also construct a few views on top of the users and recipients table that join with the correspondent tables.

To construct these materialized tables, run:

  $ node rebuild_derived_tables.js

### Run the Invariant Checks

Redshift does not systematically check or enforce primary key constraints.  Nevertheless, our data model does ensure certain primary key constraints, and the correctness of many analytic queries invariably depends on these constraints.
Similarly, some queries depend on certain columns being all lower-case.
We have developed a small script that can be run to check that these constraints hold.

  $ node runInvariantChecks.js

