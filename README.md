# Twitch - Data Analytics

This repository walks through the step-by-step process of retrieving raw Twitch stream data and transforming it into fully developed metrics.

**TL;DR:** Twitch provides a snapshot of all currently live streams, including stream metadata and current viewer counts, and that's it.  
Using this limited data, we can derive metrics as granular as how many hours a game was played, how it ranked among others, or how well a streamer is performing compared to their peers.

### Raw Data
![Twitch](https://github.com/gustavo-alvarenga/About-me/blob/main/Twitch%20Streams.png)

### Final Result (Click to view the interactive [dashboard](https://app.powerbi.com/view?r=eyJrIjoiZWI2Y2M2MTgtMDYzZS00ZjBlLTlhMzAtNmJiZmVhMzdjMTBmIiwidCI6ImI1NzZhZTMzLWM3MzAtNDk5Ny1iZWY3LTQxODkxMjQzZGJkZSJ9))
![Twitch](https://github.com/gustavo-alvarenga/About-me/blob/main/Twitch%20Streams%20Dashboard.png)

### Tools & Tech Used:
* Python
* Google Cloud Platform  
  * Compute Engine  
  * BigQuery  
  * Secret Manager
* Microsoft Fabric  
  * Dataflows  
  * Power BI
* Power Platform  
  * Power Automate

## Step #1: Retrieve Streams from Twitch

First, we need to retrieve stream data from Twitch. If you're unfamiliar with the [Get Streams](https://dev.twitch.tv/docs/api/reference/#get-streams) endpoint, it returns a list of all live streams at the time of the request.

To achieve granular tracking, you'll need a virtual machine running continuously. For better coverage, you can deploy multiple instances to reduce the interval between data points.

Set up a virtual machine in GCP. I won't go into full detail here, but you'll need to:
* Update and upgrade the system packages
* Install Python in a virtual environment
* Install the required libraries

Optional, but recommended:
* Set up a log file and direct script output to it
* Create a service file to manage restarts, limit memory usage, etc.

In this setup, temporary data is stored in BigQuery (to be processed later).

Here’s the [code](https://github.com/gustavo-alvarenga/Twitch/blob/main/%231%20Twitch%20Streams.py).  
The script does the following:
* Retrieves data from Twitch and inserts it into a temporary BigQuery table continuously
* Manages credential refresh and standardizes data format for consistency

Assumptions:
* You have a Twitch application (Client ID and Secret)
* You're using GCP Secret Manager to store secrets (adjust accordingly if not)
* You're storing data in GCP BigQuery
* You've already created a temporary table at `PROJECT_ID.twitch_temp.livestreams`, partitioned by `started_at`. Partitioning by `started_at` is strongly recommended. If you skip it, you'll need to update the query logic in the next step

The above code runs in the virtual machine you just created

## Step #2: Process Data

The temporary table will grow quickly, and we're talking a few TiB per month. To manage that, we need to aggregate the data efficiently. You can aggregate by game, streamer, or both. For simplicity, we'll focus on aggregation by game.

Since raw data can be extremely granular, the goal is to retain detail without incurring high storage or query costs.

In this example, I use a second virtual machine to run the processing script. This is mainly for demonstration purposes. In production, a scheduled query in BigQuery would be more efficient.

After setting up a second VM (same steps as above), run this [code](https://github.com/gustavo-alvarenga/Twitch/blob/main/%232%20Processing%20Data).

Process Workflow:
* Query data from the temporary table
* Process and aggregate it as described
* Upload results to a permanent table
* Delete processed data from the temporary table to reduce storage costs

## Step #3: Data Visualization

Here, I use Microsoft Fabric Dataflows to pull data from BigQuery on a daily schedule. This allows for greater flexibility when managing and modifying Power BI models that rely on the same dataset.

I won't dive into the full setup, but to create the dataflow, log into the Power BI Service and configure it to fit your needs.

You can also use Power Automate to build [automated cloud flows](https://make.powerautomate.com/) for better control over refresh schedules and dataflow triggers.

Finally, here's an example [dashboard](https://app.powerbi.com/view?r=eyJrIjoiZWI2Y2M2MTgtMDYzZS00ZjBlLTlhMzAtNmJiZmVhMzdjMTBmIiwidCI6ImI1NzZhZTMzLWM3MzAtNDk5Ny1iZWY3LTQxODkxMjQzZGJkZSJ9) that can be built from this dataset:  
![Twitch](https://github.com/gustavo-alvarenga/About-me/blob/main/Twitch%20Streams%20Dashboard.png)

You can reach out to me on [LinkedIn](https://www.linkedin.com/in/gustavo-alvarenga/)
