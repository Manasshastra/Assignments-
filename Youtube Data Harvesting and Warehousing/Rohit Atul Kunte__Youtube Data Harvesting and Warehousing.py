#importing the necessary libraries
import streamlit as st
from streamlit_option_menu import option_menu
import pymongo
import mysql.connector as sql
import pandas as pd
import plotly.express as px
from googleapiclient.discovery import build
from datetime import datetime 

# #establishing local mongodb connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["youtube_data"]

# CONNECTING WITH MYSQL DATABASE
mydb = sql.connect(host="localhost",
                   user="root",
                   password="",
                  )
mycursor = mydb.cursor(buffered=True)

#Checking if database exists if not make 
mycursor.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
mycursor.execute("USE youtube_db")

#Creating table for channel
create_channels_table_query = """
    CREATE TABLE IF NOT EXISTS Channels (
        channel_id VARCHAR(255) PRIMARY KEY,
        channel_name VARCHAR(255),
        playlist_id VARCHAR(255),
        subscribers INT,
        views INT,
        total_videos INT,
        description TEXT,
        country VARCHAR(255)
    )
"""
# Execute the CREATE TABLE query for 'Channels' table
mycursor.execute(create_channels_table_query)

#creating table for video
create_videos_table_query = """
    CREATE TABLE IF NOT EXISTS Videos (
        video_id VARCHAR(255) PRIMARY KEY,
        channel_name VARCHAR(255),
        channel_id VARCHAR(255),
        title VARCHAR(255),
        tags TEXT,
        published_date DATETIME,
        duration VARCHAR(255),
        views INT,
        likes INT,
        comments INT,
        favorite_count INT,
        definition VARCHAR(255),
        thumbnail VARCHAR(255),
        description TEXT,
        caption_status VARCHAR(255)
    )
"""

# Execute the CREATE TABLE query for 'Videos' table
mycursor.execute(create_videos_table_query)

#Create table for comments
create_comments_table_query = """
    CREATE TABLE IF NOT EXISTS Comments (
        comment_id VARCHAR(255) PRIMARY KEY,
        video_id VARCHAR(255),
        comment_text TEXT,
        comment_author VARCHAR(255),
        comment_posted_date DATETIME,
        like_count INT,
        reply_count INT
    )
"""

# Execute the CREATE TABLE query for 'Comments' table
mycursor.execute(create_comments_table_query)

# BUILDING CONNECTION WITH YOUTUBE API
api_key = "AIzaSyBxRq82TyPSS6WnaU4CqzU2xp5v_FuLPvI" 
youtube = build('youtube','v3',developerKey=api_key)

# FUNCTION TO GET CHANNEL DETAILS
def get_channel_details(channel_id):
    ch_data = []
    response = youtube.channels().list(part = 'snippet,contentDetails,statistics',
                                     id= channel_id).execute()

    for i in range(len(response['items'])):
        data = dict(Channel_id = channel_id[i],
                    Channel_name = response['items'][i]['snippet']['title'],
                    Playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                    Subscribers = response['items'][i]['statistics']['subscriberCount'],
                    Views = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    Description = response['items'][i]['snippet']['description'],
                    Country = response['items'][i]['snippet'].get('country')
                    )
        ch_data.append(data)
    return ch_data

# FUNCTION TO GET VIDEO IDS
def get_channel_videos(channel_id):
    v_ids = []
    # get Uploads playlist id
    resp = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = resp['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        resp = youtube.playlistItems().list(playlistId=playlist_id, 
                                           part='snippet', 
                                           maxResults=30,
                                           pageToken=next_page_token).execute()
        
        for i in range(len(resp['items'])):
            v_ids.append(resp['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = resp.get('nextPageToken')
        
        if next_page_token is None:
            break
    return v_ids

# FUNCTION TO GET VIDEO DETAILS
def get_video_details(v_ids):
    v_stats = []
    
    for i in range(0, len(v_ids), 50):
        resp = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=','.join(v_ids[i:i+50])).execute()
        
        for video in resp['items']:
            v_details = dict(Channel_name = video['snippet']['channelTitle'],
                                Channel_id = video['snippet']['channelId'],
                                Video_id = video['id'],
                                Title = video['snippet']['title'],
                                Tags = ",".join(video['snippet'].get('tags', [])),
                                Published_date = video['snippet']['publishedAt'],
                                Duration = video['contentDetails']['duration'],
                                Views = video['statistics']['viewCount'],
                                Likes = video['statistics'].get('likeCount'),
                               # Dislikes=video['statistics'].get('dislikeCount', 0),
                                Comments = video['statistics'].get('commentCount'),
                                Favorite_count = video['statistics']['favoriteCount'],
                                Definition = video['contentDetails']['definition'],
                                Thumbnail = video['snippet']['thumbnails']['default']['url'],
                                Description = video['snippet']['description'],
                                Caption_status = video['contentDetails']['caption']
                               )
            v_stats.append(v_details)
    return v_stats

# FUNCTION TO GET COMMENT DETAILS
def get_comments_details(v_id):
    cmt_data = []
    try:
        next_page_token = None
        while True:
            resp = youtube.commentThreads().list(part="snippet,replies",
                                                    videoId=v_id,
                                                    maxResults=70,
                                                    pageToken=next_page_token).execute()
            for cmt in resp['items']:
                data = dict(Comment_id = cmt['id'],
                            Video_id = cmt['snippet']['videoId'],
                            Comment_text = cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author = cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date = cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count = cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count = cmt['snippet']['totalReplyCount']
                           )
                cmt_data.append(data)
            next_page_token = resp.get('nextPageToken')
            if next_page_token is None:
                break
    except:
        pass
    return cmt_data

# FUNCTION TO GET CHANNEL NAMES FROM MONGODB
def channel_names():   
    ch_name = []
    for i in db.channel_details.find():
        ch_name.append(i['Channel_name'])
    return ch_name

def data_prep_for_sql(data):
    n_data = []
    for key, value in data.items():
        if isinstance(value, list):
            n_data.append(','.join(map(str, value)))
        elif key == 'Published_date' or key == 'Comment_posted_date':
            # Parse the date string from YouTube API
            parsed_date = datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')  
            # Format it in MySQL's expected format
            formatted_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
            n_data.append(formatted_date)
        else:
            n_data.append(value)
    print(f"Transformed data: {tuple(n_data)}")
    return tuple(n_data)

# SETTING PAGE CONFIGURATIONS
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing | By Rohit Atul Kunte",
                   layout= "wide",
                   initial_sidebar_state= "expanded",
                   menu_items={'About': """# This app is created to analysis youtube channel related data and store it. This is made by Rohit Atul Kunte.!*"""})


# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home","Extract and Transform","Analytics"], 
                           icons=["house","tools","card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "18px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "#C80101"},
                                   "icon": {"font-size": "25px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "green"}})

#Home page
if selected == "Home":
    col1,col2 =st.columns(2,gap = 'medium')
    col1.markdown("#### :green[Domain] : Social Media")
    col1.markdown("#### :green[Technologies used] : Python,MongoDB, Youtube Data API, MySql, Streamlit")
    col1.markdown("#### :green[Overview] : Retrieving the Youtube channels data from the Google API, storing it in a MongoDB as data lake, migrating and transforming data into a SQL database,then querying the data and displaying it in the Streamlit app.")
    col2.markdown("###### :blue[Github for the source code]: https://github.com/Manasshastra/Assignments-/tree/475c4af1ff18b3395750d11c4dcad9695e07b8b9/Youtube%20Data%20Harvesting%20and%20Warehousing")
    col2.markdown("#   ")
    col2.markdown("#   ")

#Extract and Transform
if selected == "Extract and Transform":
    st.write('## :orange[Select the operation you want to perform]')
    operation = st.selectbox(
        'Operations',
        ['📜 EXTRACT DATA', '🔥 TRANSFORM DATA']
    )
    
    # EXTRACT TAB
    if operation == '📜 EXTRACT DATA':
        st.markdown("#    ")
        st.write("### :violet[Enter the YouTube Channel_ID below :]")
        ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id")
        
        if ch_id and st.button("Extract Data"):
            ch_details = get_channel_details(ch_id)
            st.write(f'#### Data is extracted from :green["{ch_details[0]["Channel_name"]}"] channel')
            st.table(ch_details)
        
        if st.button("Data upload to MongoDB"):
            with st.spinner('Your request is under process. NOTE: If the channel has large data it may take a while to get uploaded. Please bear with it. Thank you.:))'):
                ch_details = get_channel_details(ch_id)
                v_ids = get_channel_videos(ch_id)
                vid_details = get_video_details(v_ids)  
                
                def comments():
                    cmt_d = []
                    for i in v_ids:
                        cmt_d+= get_comments_details(i)
                    return cmt_d
                cmt_details = comments()

                collections1 = db.channel_details
                collections1.insert_many(ch_details)

                collections2 = db.video_details
                collections2.insert_many(vid_details)

                collections3 = db.comments_details
                collections3.insert_many(cmt_details)
                st.success("Upload to MongoDB successful !!")
        
    #Transform tab
    elif operation == '🔥 TRANSFORM DATA':   
            st.markdown("#   ")
            st.markdown("### :violet[Select the channel which you want to transform to SQL]")
            ch_names = channel_names()  
            user_input = st.selectbox("Select channel",options= ch_names)

            def insert_in_channels():
                collections = db.channel_details
                query = """INSERT INTO Channels (channel_id,channel_name,playlist_id,subscribers,views,total_videos,description,country)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""
                for i in collections.find({"Channel_name": user_input},{'_id':0}):
                    transformed_data = data_prep_for_sql(i)
                    channel_id = transformed_data[0]
                    mycursor.execute("SELECT COUNT(*) FROM channels WHERE channel_id = %s",(channel_id,))
                    count = mycursor.fetchone()[0]
                    if count == 0:
                    #checking if it already exists, if not then insert.
                        try:
                            mycursor.execute(query,transformed_data)
                            mydb.commit()
                        except Exception as e:
                            st.error(f"There was a failure in inserting the data:{transformed_data}.Error:{e}")
                            
            def insert_in_videos():
                collections1 = db.video_details
                query1 = """INSERT INTO videos (channel_name,channel_id,video_id,title,tags,published_date,
                    duration,views,likes,comments,favorite_count,definition,thumbnail,description,caption_status) 
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
                for i in collections1.find({"Channel_name" : user_input},{'_id':0}):
                    values = []
                    for val in i.values():
                        if isinstance(val, str):
                            values.append(str(val).replace("'", "''").replace('"', '""')) 
                    else:
                        values.append(val)

                    transformed_data = data_prep_for_sql(i)
                    try:
                        mycursor.execute(query1, transformed_data)
                        mydb.commit()
                    except Exception as e:
                        st.error(f"There was a failure in inserting the data: {transformed_data}. Error: {e}")
            
            def insert_in_comments():
                collections1 = db.video_details
                collections2 = db.comments_details
                query2 = """INSERT INTO comments (comment_id,video_id,comment_text,comment_author,comment_posted_date,like_count,reply_count)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)"""
                for vid in collections1.find({"Channel_name": user_input},{'_id':0}):
                    for i in collections2.find({'Video_id': vid['Video_id']},{'_id':0}):
                        transformed_data = data_prep_for_sql(i)
                        try:
                            mycursor.execute(query2,transformed_data)
                            mydb.commit()
                        except Exception as e:
                            st.error(f"There was a failure in inserting the data:{transformed_data}.Error:{e}")
                            
            if st.button ("Submit"):
                try:
                    insert_in_channels()
                    insert_in_videos()
                    insert_in_comments()
                    st.success("The Transformation to MYSQL was carried out as expected. Success.!!")
                except Exception as e:
                    st.error(f"Error message:{e}")
    else:
        st.warning("Please select an operation to perform.")

#Analytics
if selected == "Analytics":
    st.write('## :orange[Select the question you want to see Insight for the data]')
    questions = st.selectbox('Questions',
    ['1.What are the names of all the videos and their corresponding channels?',
     '2.Which channels have the most number of videos, and how many videos do they have?',
     '3.What are the top 10 most viewed videos and their respective channels?',
     '4.How many comments were made on each video, and what are their corresponding video names?',
     '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
     '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
     '7.What is the total number of views for each channel, and what are their corresponding channel names?',
     '8.What are the names of all the channels that have published videos in the year 2022?',
     '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
     '10.Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if questions == '1.What are the names of all the videos and their corresponding channels?':
        mycursor.execute("""SELECT title as Video_name,channel_name as Channel_name
                            FROM videos
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write('### :blue[Names of all the videos and their corresponding channels.]')
        st.write(df)

    elif questions == '2.Which channels have the most number of videos, and how many videos do they have?':
        mycursor.execute("""SELECT channel_name as Channel_name, Total_videos 
                            FROM channels
                            ORDER BY Total_videos DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :blue[No. of videos in each channel:]")
        fig = px.bar(df,
                    x = mycursor.column_names[0],
                    y = mycursor.column_names[1],
                    orientation = 'v',
                    color = mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '3.What are the top 10 most viewed videos and their respective channels?':
        mycursor.execute("""SELECT channel_name as Channel_name, title as Video_title,views as Views 
                            FROM videos
                            ORDER BY views DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :blue[Top 10 most viewed videos:]")
        fig = px.bar(df,
                    x=mycursor.column_names[2],
                    y=mycursor.column_names[1],
                    orientation='h',
                    color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '4.How many comments were made on each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT a.video_id as Video_id, a.title as Video_Title, b.Total_comments
                            FROM videos as a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) as Total_comments
                            FROM comments GROUP BY video_id) as b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        print("Data retrieved for question 4.")

    elif questions == '5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name as Channel_name,title as Title,likes as Count_of_likes 
                            FROM videos
                            ORDER BY likes DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :blue[These are the top 10 most liked videos:]")
        fig = px.bar(df,
                     x=mycursor.column_names[2],
                     y=mycursor.column_names[1],
                     orientation='h',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        mycursor.execute("""SELECT title as Title, likes as Num_of_likes
                            FROM videos
                            ORDER BY likes DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '7.What is the total number of views for each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name as Channel_name, views as Views
                            FROM channels
                            ORDER BY views DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :blue[Channels vs Views :]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '8.What are the names of all the channels that have published videos in the year 2022?':
        mycursor.execute("""SELECT channel_name AS Channel_name
                            FROM videos
                            WHERE published_date LIKE '2022%'
                            GROUP BY channel_name
                            ORDER BY channel_name""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)

    elif questions == '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name as Channel_name,
                            AVG(duration)/60 as "Average_Video_Duration (mins)"
                            FROM videos
                            GROUP BY channel_name
                            ORDER BY AVG(duration)/60 DESC""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :blue[Avg video duration for channels:]")
        fig = px.bar(df,
                     x=mycursor.column_names[0],
                     y=mycursor.column_names[1],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        mycursor.execute("""SELECT channel_name as Channel_name,video_id as Video_id,comments as Comments
                            FROM videos
                            ORDER BY comments DESC
                            LIMIT 10""")
        df = pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
        st.write(df)
        st.write("### :blue[Videos which have been commented the most on:]")
        fig = px.bar(df,
                     x=mycursor.column_names[1],
                     y=mycursor.column_names[2],
                     orientation='v',
                     color=mycursor.column_names[0]
                    )
        st.plotly_chart(fig,use_container_width=True)