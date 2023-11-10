#!/usr/bin/env python
# coding: utf-8

# ### Launch spark environment

# In[1]:


spark


# ### Set up data configuration

# In[2]:


blob_account_name = "marckvnonprodblob"
blob_container_name = "bigdata"
# read only
blob_sas_token = "?sv=2021-10-04&st=2023-10-04T01%3A42%3A59Z&se=2024-01-02T02%3A42%3A00Z&sr=c&sp=rlf&sig=w3CH9MbCOpwO7DtHlrahc7AlRPxSZZb8MOgS6TaXLzI%3D"

wasbs_base_url = (
    f"wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/"
)
spark.conf.set(
    f"fs.azure.sas.{blob_container_name}.{blob_account_name}.blob.core.windows.net",
    blob_sas_token,
)


# ### Only read data from 2023/01 to 2023/03 of Comments Data

# In[3]:


comments_path = "reddit-parquet/comments/year=2023/month=[1-3]/"


# In[4]:


comments_df = spark.read.parquet(f"{wasbs_base_url}{comments_path}")


# In[5]:


comments_df.printSchema()


# In[6]:


comments_row_count = comments_df.count()
comment_col_count = len(comments_df.columns)
print(f"shape of the comments dataframe is {comments_row_count:,}x{comment_col_count}")


# In[7]:


comments_df.rdd.getNumPartitions()


# In[8]:


comments_df.show(5)


# ## Saving data to Azure Storage

# In[19]:


# from pyspark.sql.functions import lower
# from pyspark.sql.functions import lit
# comments_filtered = comments_df.filter(lower(comments_df.subreddit) == lower(lit("SoCCer")))

from pyspark.sql.functions import lower, col

#list of subreddits
subreddits = [
    "soccer", "mls", "premierleague", "laliga",
    "bundesliga", "seriea", "ligue1", "chelseafc", "reddevils",
    "gunners", "liverpoolfc", "coys", "mcfc", "barca",
    "realmadrid", "acmilan", "juve", "asroma", "fcbayern",
    "ussoccer", "nwsl", "soccernerd", "soccercirclejerk",
    "footballtactics", "footballhighlights",
    "worldcup", "europaleague", "championsleague",
    "casualsoccer", "footballmedia",
    "borussiadortmund", "schalke04", "atletico", "psg", "ajaxamsterdam", "celticfc"
]


#convert subreddit column in DataFrame to lowercase and filter
comments_filtered = comments_df.filter(lower(col("subreddit")).isin(subreddits))


# In[20]:


# List of columns to drop
columns_to_drop = [
    "author_cakeday",
    "author_flair_css_class",
    "can_gild",
    "distinguished",
    "edited",
    "id",
    "is_submitter",
    "parent_id",
    "link_id",
    "permalink",
    "retrieved_on",
    "subreddit_id"
]

#drop the columns from the DataFrame that are not needed
comments_filtered = comments_filtered.drop(*columns_to_drop)


# In[21]:


#show the remaining schema
comments_filtered.printSchema()


# In[26]:


comments_filtered.count()


# In[27]:


from pyspark.sql import functions as F

#test the minimum and maximun created_utc
min_created_utc = comments_df.agg(F.min("created_utc")).first()[0]
max_created_utc = comments_df.agg(F.max("created_utc")).first()[0]

print(f"The minimum created_utc is: {min_created_utc}")
print(f"The maximun created_utc is: {max_created_utc}")


# In[28]:


comments_filtered.show(5)


# In[18]:


#show unique values in the "subreddit" column
unique_subreddits = comments_filtered.select("subreddit").distinct()


# In[24]:


unique_subreddits.show(30)


# ### Save the filter dataset

# In[29]:


workspace_default_storage_account = "group08astoragec0a5c9b39"
workspace_default_container = "azureml-blobstore-8f67895d-e507-48c5-8b8e-f003f0227b44"

workspace_wasbs_base_url = (
    f"wasbs://{workspace_default_container}@{workspace_default_storage_account}.blob.core.windows.net/"
)
# save
comments_filtered.write.mode("overwrite").parquet(f"{workspace_wasbs_base_url}<PATH-TO-READ/WRITE>")


# ### check if data is in the blob stoarge.

# In[30]:


df = spark.read.parquet((f"{workspace_wasbs_base_url}<PATH-TO-READ/WRITE>"))


# In[31]:


df.show()


# In[32]:


df.count()


# In[ ]:




