# Analytic Inbox

Analytic Inbox (ai) is a service for analysis and visualization of communication between people over electronic media such as email.

## Goals

We'd like to be able to perform analysis like:

   * Who do I correspond directly with the most?  What is a rank-ordered list of my top correspondents?
   * What does this pattern look like over time?  Who did I used to correspond with regularly that I haven’t been in touch with in a while?
   * In time series chart / metrics, show history of interactions with a correspondent, and original (non-quoted) lines of text in each message.
   * Who do I owe email to?
   * Who owes me email?
   * What URLs have I mailed to myself?  Can we make those searchable?
   * What URLs have been sent to me by close/important correspondents?
   * Among my correspondents, what is my average response time?  What percentage of emails do I reply to?  What are average response times for each of the people I correspond with?  How does this metric change over time?
   * For a given correspondent, who are 'adjacent correspondents' that appear on To/Cc line in our interactions.  How often / frequently do they appear?

# Architecture

The Analytic Inbox service is shown here:

![Analytic Inbox Architecture](doc/images/architecture.png "ai architecture")

The light gray boxes indicate likely data center / WAN boundaries.
This diagram captures the basic structure but omits some essential details, such as OAuth authentication dependencies or use of S3 for loading files into RedShift.

The Postgres database stores metadata such as user accounts and preferences, and application data such as user-generated cleaned up contact data.

Redshift is the analytics data warehouse.

# Repository Structure

The folder structure of this repository reflects the architecture and is organized as follows:

- **doc/** - project documentation, screenshots, etc.
- **db/** - Setup and maintenance scripts for Postgres database
- **analytics_db/** -- Setup and maintenance scripts for analytics data warehouse
- **analytics_db/queries** --
- **indexer/** - The indexer service, responsible for transferring meta-data from an email server in to RedShift. 
- **frontend/** - The client and server components of the service front-end
- **frontend/server** - Web server front end
- **frontend/server/views** - HTML templates for static content
- **frontend/client** - JavaScript code for web client, built with React.js

# Setup

You will need python 2.7.8 installed locally.

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

In your RedShift SQL client, execute the commands in [analytics_db/create_users_table.sql](analytics_db/create_users_table.sql)

