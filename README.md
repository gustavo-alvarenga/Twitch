# Twitch - Data Analytics

TL;DR: This repository will provide code to retrieve data from Twitch to gain insights as granular as which game was played by which streamer, for how many seconds, on which day, and the average viewers and hours watched.

### Tools & Tech used:
- Python
- Google Cloud Platform
  - Virtual Machines
  - BigQuery
  - Secret Manager
- Power BI
 

## Step #1: Retrieve streams from Twitch

First, we need to retrieve stream data from Twitch. If you are unfamiliar with the [Get Streams](https://dev.twitch.tv/docs/api/reference/#get-streams) endpoint, it retrieves a list of all streams that are live at any specific moment. To be as granular as possible, we need to have a virtual instance running at all times. For better results, you can set up multiple instances to reduce the interval between updates.

In this setup, I chose to store the temporary data (as we're going to process this data later) in BigQuery, though it could be stored in Data Lakes as well.

Here's the [code](https://github.com/gustavo-alvarenga/Twitch/blob/main/%231%20Twitch%20Streams.py). Here are a few assumptions:
- You already have a Twitch application (Client ID and Secret). If not, create one here.
- You are using GCP's Secret Manager to store secrets. If not, make the necessary adjustments.
- You are using GCP's BigQuery to store data.
- The created temporary table (PROJECT_ID.twitch_temp.livestreams) is partitioned by started_at. While this isn't mandatory, it will require further adjustment in the query statements in the processing code below.

## Step #2: Process Data

Because the data can become too granular, we need to consolidate it to be as detailed as needed without significantly increasing storage/querying costs. Here are the consolidation "rules":
- Each row will identify a game played in a stream. This row contains information about the game played, how many seconds were streamed, and how many seconds were watched by the viewers, plus information regarding the stream and streamer.
- Each stream will contain at least one row per game played. If the streamer played game A, then B, and then C, that stream will contain 3 rows. If the streamer played game A, then B, then back to A, the stream will also contain 3 rows.

Here's the [code](https://github.com/gustavo-alvarenga/Twitch/blob/main/%232%20Processing%20Data). The workflow of this process is as follows:
- Query data from the Step #1 temporary table
- Process data as described above
- Upload to the permanent table
- Delete data from the temporary table

## Step #3: Data Visualization

While there could be other intermediary steps, I chose to create a Power BI report to showcase the data. Since GitHub won't accept iframes, I won't be able to link the actual report here but will provide a screenshot along with a link to my [portfolio](https://www.ggsanalytics.com/homepage/).

All the below reports are samples of what can be created from the data retrieved as described above.

### Creator PoV [Report](https://www.ggsanalytics.com/dashboards-free/)
[![Creator PoV - Power BI Report](https://www.ggsanalytics.com/wp-content/uploads/2024/04/dashboard-image.png)](https://www.ggsanalytics.com/dashboards-free/)

### Game PoV [Report](https://www.ggsanalytics.com/dashboards-free/) (gif)
[![Recording GIF](https://www.ggsanalytics.com/wp-content/uploads/2024/05/Recording2024-05-13173035-ezgif.com-video-to-gif-converter.gif)](https://www.ggsanalytics.com/dashboards-free/)

## That's all, folks!

If you have any questions, reach out to me on [LinkedIn](https://www.linkedin.com/in/gustavo-alvarenga/)


