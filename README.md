
## ✨🔄 “Full Automation in Real-Time Streaming! From Kafka to Google Cloud with 📊 Spark, 🪄 Terraform, 🔮 Mage AI & 🛠 DBT: Powering Dashboards On-the-Fly!” 🚀📈🔁

![](https://cdn-images-1.medium.com/max/3840/1*088DIcBFHCJqgN9YjTLK-A.png)

## Introduction:

In the ever-evolving landscape of data and technology, real-time streaming has emerged as a game-changer for businesses aiming to gain a competitive edge. This technological prowess becomes especially vital in dynamic environments like train stations, where a myriad of events, from train movements to unforeseen incidents, occur concurrently. Dive deep into a simulated real-time train station environment, leveraging the robust capabilities of Kafka, and witness the magic behind a fully automated data pipeline. This pipeline, starting its journey from Kafka topics, navigates through tools like Spark for data processing, Terraform for seamless cloud deployments, and Mage AI for data ingestion into BigQuery. It eventually culminates in visually engaging dashboards on Google Data Studio (Looker), aided by DBT’s crafting of meaningful metrics. As the spotlight shines on data-driven decision-making in modern organizations, this exploration underscores how combining real-time streaming with cutting-edge technologies not only drives efficiency but also fosters innovation in managing sophisticated systems like modern train stations.

![](https://cdn-images-1.medium.com/max/2224/1*FPmJoV_ygWq1tsXCvBzq4Q.png)

## Technological Stack and Their Roles in the Data Pipeline

Confluent Cloud 🌩:

* Task Description: Confluent Cloud, a more advanced version of Kafka, serves as the main ingestion platform. It captures real-time data from a range of train station events and routes it through topics, ensuring a structured data flow within the pipeline.

Spark on Dataproc ⚡:

* Task Description: After data ingestion via Confluent Cloud, Spark, operating on Google Cloud’s Dataproc, takes over the processing. Capable of efficiently managing vast data volumes, Spark transforms the streaming raw data into structured formats ideal for subsequent analysis and then stores it within Cloud Storage.

Cloud Storage ☁️📦:

* Task Description: Google Cloud Storage is the go-to interim storage solution for the processed data. Post-transformation by Spark, the data is stored here, waiting for Mage AI and other pipeline components to act upon it.

Terraform 🌐:

* Task Description: Terraform is pivotal for seamless cloud deployments. Offering Infrastructure as Code (IaC), it ensures the dependable and consistent deployment of data infrastructure components, such as Cloud Storage, BigQuery, and Dataproc.

Mage AI 🧙‍♂️:

* Task Description: Mage AI plays a dual role. Firstly, it sets up an external table, taking data processed by Spark from Cloud Storage and pushing it into BigQuery. Secondly, it orchestrates all DBT tasks, guaranteeing fluid data integration into analytical tools.

BigQuery 📈:

* Task Description: Google BigQuery stands as the analytical data warehouse. Inside BigQuery, there are two principal databases:

* staging_data: This database hosts preliminary data, preparing it for final processing.

* prod_data: This is where the polished and processed data resides, making it accessible for analytics and visualization tasks.

Google Data Studio (Looker) 📊:

* Task Description: When fused with Google Data Studio, Looker converts the data from BigQuery’s prod_data database into visually compelling narratives, transforming pure insights into actionable decisions.

DBT (Data Build Tool) 🔧:

* Task Description: DBT further refines the data present in the prod_data and staging_data databases within BigQuery. It morphs datasets into models tailored for business intelligence functions and assures that the dashboards reflect precise and insightful data.

## Picture a Bustling Train Station

Imagine a bustling train station, a place where thousands of passengers transit every day, trains come and go, and where technology plays a pivotal role in ensuring smooth operations.

Times of Day:

* Our train station experiences peak hours between 7–9 AM in the morning and 5–7 PM in the evening. During these times, the influx of passengers and train activity surges.

Train Events:

* Trains arrive and depart consistently. However, when an incident occurs at a station, trains might skip stopping at that station to ensure safety.

Incidents:

* Every so often, there might be incidents at the station, like a suspicious package or a technical issue. These incidents can range from minor to major and can influence the movement of both trains and passengers. The exact location of the incident (such as the platform or corridor) is also noted.

Equipment Malfunctions:

* The station is furnished with various equipment like escalators, display boards, and ticket validators. Occasionally, these pieces of equipment might break down, and this is promptly logged.

Passenger Flow:

* Passengers are constantly entering and exiting. When a train arrives, some passengers disembark while others board. If an incident takes place, it might diminish the flow of passengers since some might opt to wait or take an alternative route.

Ticket Validation:

* Before boarding, passengers validate their tickets. The count of validated tickets is tracked for each train arrival.

Communication with a Central System:

* All these events are instantaneously relayed to a central system (in this case, Kafka) for real-time analysis and response. For instance, if an escalator is down, a maintenance team might be immediately alerted.

Continuous Updates:

* Every three minutes, our simulation scenario restarts, producing new events and mirroring a new time of the day. This paints a picture of an incessant stream of activity at the station, just as in a real, operational train station.


## Simulation Script Outputs

The script simulates events related to a train station system, then sends this data to a Kafka system and also locally stores them as JSON files.

Here are the main outputs:

Incidents:

* Description: Incidents occurring in the stations.

* Attributes:

* *id*: Incident identifier.

* *type*: Type of incident (e.g., “suspect package”, “technical”).

* *station*: Station where the incident occurred.

* *location*: Specific location of the incident (e.g., Dock, corridor, etc.)

* *timestamp*: Time when the incident occurred.

* *severity*: Severity of the incident (minor or major).

* *duration*: Expected duration of the incident.

Equipment Failures:

* Description: Equipment malfunctions occurring in the stations.

* Attributes:

* *id*: Failure identifier.

* *equipment*: Type of failed equipment (validator, escalator, display board, etc.).

* *station*: Station where the malfunction occurred.

* *location*: Specific location of the malfunction.

* *timestamp*: Time when the malfunction occurred.

* *duration*: Expected duration of the malfunction.

Train Arrivals:

* Description: Trains arriving at the stations.

* Attributes:

* *id*: Arrival identifier.

* *station*: Station where the train arrived.

* *timestamp*: Arrival time of the train.

* *passengers*: Number of passengers on board.

* *dock_number*: Dock number where the train arrived.

* *wagon_count*: Number of train wagons.

* *direction*: Direction of the train (North, South, East, West).

Train Departures:

* Similar to “Train Arrivals” but pertains to trains departing the stations.

Ticket Validations:

* Description: Recording of the number of validated tickets for a specific train.

* Attributes:

* *id*: Validation identifier.

* *train_id*: Identifier of the train associated with the validation.

* *station*: Station where the validation occurred.

* *timestamp*: Time of validation.

* *validations*: Number of validated tickets.

Passenger Flow:

* Description: Information about passenger flow related to a specific train.

* Attributes:

* *id*: Flow identifier.

* *train_id*: Identifier of the associated train.

* *station*: Station concerned by the passenger flow.

* *timestamp*: Time when the flow information was recorded.

* *waiting_time*: Average waiting time for passengers.

* *ticket_check_time*: Average time needed to validate a ticket.

* *reduced_flow_factor*: Flow reduction factor (e.g., in case of an incident).

In addition, the script stores this data in respective JSON files (incidents.json, equipment_failures.json, etc.) and also sends it to specific Kafka topics for later use or real-time analysis.

## Metrics for Dashboard:

The dashboard will provide an overview of various key performance indicators and metrics related to the operations of a transport or train system. Here’s a breakdown of the metrics we’ll create:

**Passenger Flow** (Flux des passagers):

* Total Number of Passengers: Real-time data on the number of passengers both entering and exiting the system.

* Peak Traffic Locations: Identify the stations or entry/exit points experiencing the highest and lowest passenger flow.

**Ticket Validations** (Validations de billets):

* Total Tickets Validated: Real-time data on the number of tickets that have been validated.

* Invalid or Rejected Tickets Rate: Percentage or rate of tickets that were not valid or were rejected upon validation.

**Train Departures and Arrivals** (Départs et arrivées de trains):

* Total Departures and Arrivals: Real-time data on the number of trains that have departed or arrived.

* Train Punctuality Rate: Measure the punctuality of trains. For instance, the percentage of trains arriving on time versus those arriving late.

**Incidents **(Incidents):

* Total Reported Incidents: Real-time data on the number of incidents reported.

* Most Common Incident Types: Classification of incidents, such as technical malfunctions, security breaches, etc.

* Incident Hotspots: Identify zones or stations with the highest number of reported incidents.

**Equipment Failures** (Défaillances d’équipement):

* Total Reported Equipment Failures: Real-time data on the number of equipment failures reported.

* Most Common Failure Types: Classification of failures, such as escalator malfunctions, door issues, ticketing system glitches, etc.

* Equipment Failure Hotspots: Identify zones or stations with the highest number of equipment failures.

## Terraform:

## Terraform Configuration Context:

Terraform is a widely-used tool for Infrastructure as Code (IaC) that allows developers and operators to provision and manage infrastructure using declarative configuration files. In the provided configuration, we’re leveraging the Google Cloud Platform (GCP) provider to create various resources, mainly focused on data processing and storage. Let’s dive deeper into the components:

 1. Provider Configuration:

* provider "google": This block sets up and configures the GCP provider.

* credentials: Path to a JSON file containing the Google Cloud credentials.

* project: The GCP project ID.

* region: Specifies the region where the resources will be created.

Google Cloud Storage Bucket:

* google_storage_bucket "dataproc_staging_bucket": This block creates a Cloud Storage bucket.

* name: Name of the bucket.

* location: Geographical location of the bucket. Here, it's set to "US".

* force_destroy: This argument ensures that the bucket will be destroyed even if it contains objects.

Google Dataproc Cluster:

* google_dataproc_cluster "mulitnode_spark_cluster": This block provisions a Google Dataproc cluster for running Apache Spark and Hadoop jobs.

* name: Name of the cluster.

* region: Region where the cluster will be deployed.

* cluster_config: Contains multiple nested arguments, such as:

* staging_bucket: The Cloud Storage bucket used for staging.

* gce_cluster_config: GCE-related configurations, like the zone and network details.

* master_config and worker_config: Define the number of instances, machine types, and disk configurations for master and worker nodes, respectively.

* software_config: Specifies software-related configurations like image version and optional components.

Google BigQuery Datasets:

* google_bigquery_dataset "stg_dataset" & google_bigquery_dataset "prod_dataset": These blocks create datasets in Google BigQuery, which is a serverless, highly-scalable, and cost-effective multi-cloud data warehouse.

* dataset_id: Unique ID for the dataset.

* project: The GCP project ID.

* location: Specifies the region where the dataset will be created.

* delete_contents_on_destroy: This argument ensures that all contents within the dataset are deleted before destroying the dataset.

## Kafka Stream Processor: General Context

Here’s a general overview of the script’s functionalities:

Initialization and Configuration:

* The script initializes a KafkaStreamProcessor class that handles the interaction with Kafka.

* Configuration properties for the Kafka cluster, such as bootstrap servers and security settings, are read from a provided file.

Spark Session Creation:

* A SparkSession is the entry point to using Spark functionality. The script offers a method to either create a new SparkSession or get an existing one.

Kafka Stream Reading:

* The script sets up streaming data ingestion from Kafka using Spark’s structured streaming capabilities.

* Each Kafka record is converted from its raw format into structured rows using provided schemas. This allows more complex transformations and analyses to be applied to the incoming data.

Stream Processing:

* Incoming data streams are cleaned, and the necessary transformations are applied.

* The timestamp columns are adjusted.

* Any batches that are empty are identified and ignored.

Stream Persistence:

* The processed data streams are written to a storage path (in this example, it looks like Google Cloud Storage). This is essential for persisting the processed data for further analysis or serving to other applications.

* The output data format is specified, with “parquet” being the default format, which is a columnar storage format offering high performance for analytics workloads.

Debugging and Monitoring:

* The script includes some debugging functionalities, such as logging the content of an in-memory table to monitor the processed data.

* Logging is set up throughout the script to notify the user about the progress and any potential issues.

Execution:

* The main execution logic is encapsulated in the run method of the KafkaStreamProcessor class. This method sets up the streams, initiates the reading and writing processes, and waits for any of the streams to terminate.

* The script execution begins from the if __name__ == "__main__": block, ensuring that if this script is imported elsewhere, the main execution logic won't run automatically.


## Prerequisites:

Confluent Cloud:

* You will need an account with Confluent to access Kafka services. Sign up for free on [Confluent Cloud](https://www.confluent.io/get-started/?product=cloud) for a 30-day trial period. This signup provides you with $400 of free credits, and no credit card is required for account creation.

* If you prefer not to use Confluent Cloud, there’s an option to run a local Kafka cluster using Docker.

* An account on Google Cloud Platform (GCP) is necessary. Ensure you have the necessary permissions to create and manage resources.

* [Terraform](https://www.terraform.io/) needs to be installed on your machine. Terraform is a tool that allows you to create, change, and version infrastructure safely and efficiently. It will be used to deploy and manage resources on GCP.

* [Mage AI](https://mage.ai/) must be installed on your machine.

* [dbt](https://www.getdbt.com/) must be installed on your machine. dbt is a tool that lets analysts and engineers transform, test, and document data in data warehouses.

## Getting Started with the Project

### Step 1: Set Up Infrastructure with Terraform

 1. Clone the repository:

    git clone https://github.com/Stefen-Taime/railway-station-streaming.git

 1. Navigate to the project directory

    cd railway-station-streaming

 1. Enter the terraform directory and modify the variables.tf with the appropriate values. Make sure to place your Google JSON credentials file in the same directory.

 2. Initialize and apply Terraform configurations:

    terraform init
    terraform plan
    terraform validate
    terraform apply

After this, you should have a Spark cluster deployed in your GCP account, a bucket in Cloud Storage, and a data warehouse in BigQuery.

### Step 2: Set Up Kafka Topics

 1. Navigate to the confluent directory:

    cd confluent

Replace the values in the client.properties file.

Execute the script to create the Kafka topics:

    ./create_topics.sh

![](https://cdn-images-1.medium.com/max/3772/1*z4Cn2cVWAETTYTFYLLIXgQ.png)

After this, you can check your Confluent Cloud, and you should have 6 topics set up.

Now, run the data simulation script:

    python producer.py

![](https://cdn-images-1.medium.com/max/2686/1*XxgbbzkenZF_OGm8cCMoXg.png)

### Step 3: Configure and Run Spark Streaming

 1. Connect to your Spark cluster:

    gcloud compute ssh railway-station-spark-cluster-m --zone=us-central1-a --project=[YOUR GOOGLE PROJECT ID]

 1. Once connected, clone the project repository and navigate to the Spark subdirectory:

    git clone https://github.com/Stefen-Taime/railway-station-streaming.git
    cd railway-station-streaming/spark

 1. Modify the values in the client.properties file as needed.

 2. Start the Spark streaming job:

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 sparkstreaming.py

![](https://cdn-images-1.medium.com/max/2694/1*AkLAYEmiojkaqvm2xKX9-A.png)

You should see an output indicating the successful processing of the data.

![](https://cdn-images-1.medium.com/max/3134/1*zKh6MEU9l_1jRIafH9GPeA.png)

![](https://cdn-images-1.medium.com/max/3154/1*cEJBTA_XO35msuGt5gE6eA.png)

### Step 4: Load Data into BigQuery

 1. In another terminal, start the mage environment:

    mage start mage-railway-station

 1. Before proceeding, modify the config.yaml file, especially the service value for Google, and provide the path to your Google JSON credentials.

 2. Within the data_loaders directory, run the script to create an external table in BigQuery:

    python create_external_table.py

This will process real-time data from Spark and store it in Cloud Storage. After this, you can start running the dbt models. Remember to adjust the values in the profiles.yml of dbt accordingly.

![](https://cdn-images-1.medium.com/max/2000/1*Ksrmhst1NrRU6GeobREZDQ.png)

![](https://cdn-images-1.medium.com/max/2000/1*jIm0K-abJCOph3f0FOY_ow.png)

Finally, you can schedule your pipeline in mage to execute at your preferred interval (e.g., every 5 or 10 minutes) by modifying the trigger. At this stage, in BigQuery, you should see the processed data. To conclude, create a dashboard using your preferred tool, for instance, Google Data Studio.

![](https://cdn-images-1.medium.com/max/2000/1*zVC_AWeA5FQ9Yx-eAUd18Q.png)

![](https://cdn-images-1.medium.com/max/3822/1*FAI3EbdqKBhoLV7dAKCoWg.png)

![](https://cdn-images-1.medium.com/max/3840/1*BcoC34kLhMGaMwLSor7PnQ.png)

Note: There are still many improvements to be made in this project to make it closer to a production-ready project. For example, using a schema registry in Confluent Kafka, better structuring the Spark code, and more. Please don’t hesitate to share your feedback and let me know what tools you might have used instead at each step. :)

## Important Reminder: Cleanup to Avoid Unnecessary Costs

After completing your work or experiments with this project, always remember to clean up your resources to avoid incurring any unnecessary costs.

To destroy the resources created with Terraform:

 1. Navigate back to the Terraform directory:

    cd railway-station-streaming/terraform

 1. Run the following command:

    terraform destroy

This will tear down the resources that were set up with Terraform.

Disclaimer: Ensure you monitor and manage your cloud costs and usage. I am not responsible for any unexpected charges or overages that may result from using this project.
