from googleapiclient.discovery import build
import pandas as pd
import pymongo
import psycopg2
import streamlit as st
import datetime

#API_Code#
api_key='AIzaSyC9-sVtcOV15iueCSlkzBRcNSj88LXD8vs'
youtube= build('youtube','v3', developerKey=api_key)

#Gathering Data for a Youtube channel#

def get_channel_data(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_Id=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                Views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

#Gathering Video id and Video information of each video of a youtube channel#

def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                        part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None
    while True:
            response1=youtube.playlistItems().list(
                                                part='snippet',
                                                playlistId=Playlist_Id,
                                                maxResults=50,
                                                pageToken=next_page_token).execute()
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')

            if next_page_token is None:
                break
    return video_ids

#Gather Video Information from each video in a youtube channel#
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()

        for item in response["items"]:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Video_name=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views_count=item['statistics'].get('viewCount'),
                    Likes_count=item['statistics'].get('likeCount'),
                    Dislikes_count = item['statistics'].get('dislikeCount'),
                    Comments_count=item['statistics'].get('commentCount'),
                    Favorite_Count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_Status=item['contentDetails']['caption'])
            video_data.append(data)    
    return video_data

#Gathering comments data from a video of a youtube channel#
def get_comment_details(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data

#Playlist Details of a youtube channel#
def get_playlist_info(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(Playlist_Id=item['id'],
                                Video_name=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Video_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data

#Mongo Client Details and db name using clinet driver#
client=pymongo.MongoClient("mongodb+srv://shivaperiaswamy75:boWnIqYbO4KE0VQE@mongolearning.o5qrylu.mongodb.net/?retryWrites=true&w=majority&appName=mongoLearning")
db=client["youtube_harvesting_guvi"]
#Testing a sample collection insert#
#coll_sample = db["Channel_details"]
#s = {"name":"shiva","YOB": 1995}
#coll_sample.insert_one(s)

def channel_details(channel_id):
    channel_details=get_channel_data(channel_id)
    playlist_details=get_playlist_info(channel_id)
    video_ids=get_videos_ids(channel_id)
    video_details=get_video_info(video_ids)
    comment_details=get_comment_details(video_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":channel_details,"playlist_information":playlist_details,
                      "video_information":video_details,"comment_information":comment_details})
    
    return "upload completed successfully"

#Table creation for channel and data insertion into postgres DB
def channels_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Summer#2024",
                        database="youtube_harvesting_guvi",
                        port="5432")
    cursor=mydb.cursor()

    try:
        create_query='''create table if not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channels table already created")
    
        #fetching all datas
    query_1= "SELECT * FROM channels"
    cursor.execute(query_1)
    table= cursor.fetchall()
    mydb.commit()

    chann_list= []
    chann_list2= []
    df_all_channels= table
    chann_list.append(df_all_channels)
    for i in chann_list[0]:
        chann_list2.append(i)
    if channel_name_s in chann_list2:
        news= f"Your Provided Channel {channel_name_s} is Already exists"        
        return news
    else:
        channel_info= []
        coll1=db["channel_details"]
        for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
            channel_info.append(ch_data["channel_information"])
        df_channel_info= pd.DataFrame(channel_info)
        for _,row in df_channel_info.iterrows():
            input_query = '''insert into channels(Channel_Name,
                                                            Channel_Id ,
                                                            Subscribers,
                                                            Views,
                                                            Total_Videos,
                                                            Channel_Description,
                                                            Playlist_Id)values(%s,%s,%s,%s,%s,%s,%s)'''
            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_Videos'],
                    row['Channel_Description'],
                    row['Playlist_Id'])
            
            cursor.execute(input_query, values)
            mydb.commit()
            


mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="Summer#2024",
                    database="youtube_harvesting_guvi",
                    port="5432")
cursor=mydb.cursor()

#Playlist table creation and data insertion
def playlist_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Summer#2024",
                        database="youtube_harvesting_guvi",
                        port="5432")
    cursor=mydb.cursor()
    try:
        create_query='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                    Video_name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Channel_Name varchar(100),
                                                    PublishedAt TIMESTAMPTZ,
                                                    Video_Count int
                                                    )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Playlist table already created")
    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        for i in range(len(ch_data['playlist_information'])):
            single_channel_details.append(ch_data["playlist_information"][i])
    df_single_channel= pd.DataFrame(single_channel_details)
    for _,row in df_single_channel.iterrows():
        input_query = ('''insert into playlists(Playlist_Id,
                                                    Video_name,
                                                    Channel_Id,
                                                    Channel_Name,
                                                    PublishedAt,
                                                    Video_Count)
                                            values(%s,%s,%s,%s,%s,%s)''')
        values=(row['Playlist_Id'],
                row['Video_name'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Video_Count']
                )
        cursor.execute(input_query,values)
        mydb.commit()

#Video table creation and data insertion
def videos_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Summer#2024",
                        database="youtube_harvesting_guvi",
                        port="5432")
    cursor=mydb.cursor()
    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Video_name varchar(200),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views_count bigint,
                                                    Likes_count bigint,
                                                    Dislikes_count bigint,
                                                    Comments_count int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)
                                                        )'''
    cursor.execute(create_query)
    mydb.commit()

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"video_information":1}):
            for i in range(len(ch_data['video_information'])):
                single_channel_details.append(ch_data["video_information"][i])
    df_single_channel= pd.DataFrame(single_channel_details)
    for _,row in df_single_channel.iterrows():
            input_query = '''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Video_name,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views_count,
                                                    Likes_count,
                                                    Dislikes_count,
                                                    Comments_count,
                                                    Favorite_Count,
                                                    Definition,
                                                    Caption_Status) 
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Video_name'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views_count'],
                    row['Likes_count'],
                    row['Dislikes_count'],
                    row['Comments_count'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])
    cursor.execute(input_query,values)
    mydb.commit()

##comments table creation and data insertion    
def comments_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Summer#2024",
                        database="youtube_harvesting_guvi",
                        port="5432")
    cursor=mydb.cursor()
    try:
        create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )'''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Comments table already created")

    single_channel_details= []
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_channel_details.append(ch_data["comment_information"])
    df_single_channel= pd.DataFrame(single_channel_details[0])
    for _,row in df_single_channel.iterrows():
        input_query = '''insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published) 
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                   row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                    )
    cursor.execute(input_query,values)
    mydb.commit()

def tables(channel_name):

    news= channels_table(channel_name)
    if news:
        st.write(news)
    else:
        print("Table and data already available")

    return "Tables Created Successfully"

def tables1(channel_name):

    news= playlist_table(channel_name)
    if news:
        st.write(news)
    else:
        print("Table and data already available")
    return "Data inserted Successfully"

def tables2(channel_name):

    news= videos_table(channel_name)
    if news:
        st.write(news)
    else:
        print("Table and Data already available")
    return "Data Inserted Successfully"

def tables3(channel_name):

    news= comments_table(channel_name)
    if news:
        st.write(news)
    else:
        print("Table and data already available")
    return "Data inserted Successfully"

#tables to be visible in UI
def show_channels_table():
    ch_list=[]
    db=client["youtube_harvesting_guvi"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():
    pl_list=[]
    db=client["youtube_harvesting_guvi"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["youtube_harvesting_guvi"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    db=client["youtube_harvesting_guvi"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
    df3=st.dataframe(com_list)

    return df3

url = 'https://www.youtube.com'

#streamlit part
st.title(":blue[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
with st.sidebar:
     st.header("Technology used for this page")
     st.caption("Python Scripting")
     st.caption("Data Collection")
     st.caption("MongoDB")
     st.caption("API Integration")
     st.caption("Data Management using MongoDB and SQL")
     st.header("how to get a channel ID")
     st.caption("step 1: Login to youtube")
     st.caption("step 2: navigate to your favorite channel")
     st.caption("step 3: right click and view source code.")
     st.caption("step 4: search channelID and get the channel id.")
     st.markdown(f'''<a href={url}><button style="background-color:Red;">try yourself</button></a>''',unsafe_allow_html=True)

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["youtube_harvesting_guvi"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)
        
        
all_channels= []
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    all_channels.append(ch_data["channel_information"]["Channel_Name"])
        
unique_channel= st.selectbox("Select the Channel",all_channels)

if st.button("Migrate to Sql - channel_details"):
    Table=tables(unique_channel)
    st.success(Table)

if st.button("Migrate to Sql - playlist_details"):
    Table=tables1(unique_channel)
    st.success(Table)

if st.button("Migrate to Sql - videos_details"):
    Table=tables2(unique_channel)
    st.success(Table)

if st.button("Migrate to Sql - comments_details"):
    Table=tables3(unique_channel)
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()

#SQL Section
mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="Summer#2024",
                    database="youtube_harvesting_guvi",
                    port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your question",("1. All the videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with higest likes",
                                              "6. likes of all videos",
                                              "7. views of each channel",
                                              "8. videos published in the year of 2022",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))

if question=="1. All the videos and the channel name":
    query1='''select Video_name, channel_name as chanelName from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["videos","channel name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from channels 
                order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views_count as views,channel_name as channelname,video_name as videotitle from videos 
                where views_count is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comments_count as numberof_comments,video_name as videotitle from videos where comments_count is not null order by comments_count desc'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with higest likes":
    query5='''select Video_name as videotitle,channel_name as channelname,likes_count as likecount
                from videos where likes_count is not null order by likes_count desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all videos":
    query6='''select likes_count as likes,video_name as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channel":
    query7='''select channel_name as channelname ,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question=="8. videos published in the year of 2022":
    query8='''select video_name as video_title,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","published_date","channelname"])
    st.write(df8)

elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)

elif question=="10. videos with highest number of comments":
    query10='''select video_name as videotitle, channel_name as channelname,comments_count as comments from videos where comments_count is
                not null order by comments_count desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)