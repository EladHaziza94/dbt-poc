{{ config(materialized='table') }}


with fb_reaction_data as(
        select media_object_id,SUM(js.value) as total_reactions from {{ source('WSC_ENV_DB', 'MRR_SOCIAL_ENGAGEMENT_FACEBOOK') }},
        lateral flatten( input => REACTIONS ) as js group by media_object_id
    )
    
SELECT DISTINCT
  CHANNEL_ID                                                        AS CHANNEL_ID,
  CHANNEL_TITLE                                                     AS CHANNEL_TITLE,
  NULL                                                              AS CHANNEL_DISPLAY_TITLE,
  'YouTube'                                                         AS PUBLISHAPP_NAME,
  ED.VIDEO_ID                                                       AS REFERENCE_ID,
  NULL                                                              AS REFERENCE_TYPE,
  CAPTION                                                           AS REFERENCE_TITLE,
   to_timestamp(left(VIDEO_PUBLISHED_AT,19))                        AS PUBLISH_DATETIME,
  LIKES                                                             AS LIKES,
  COMMENTS                                                          AS COMMENTS,
  NULL                                                              AS ENGAGEMENT,
  VIEWS                                                             AS VIEWS,
  NULL                                                              AS IMPRESSIONS,
  NULL                                                              AS REACH,
  NULL                                                              AS RETWEETS,
  TAGS                                                              AS HASHTAGS,
  PUBLISHJOBID                                                      AS PUBLISHJOBID,
  CASE WHEN substr(DURATION,3) = 'P0D' THEN NULL 
    ELSE to_time(concat(iff(substr(DURATION,3) NOT LIKE '%H%', '00',
    iff(len(split_part(substr(DURATION,3),'H',1))>2,'00',
        split_part(substr(DURATION,3),'H',1))),':',
                        iff(substr(DURATION,3) NOT LIKE '%M%', '00',
                            iff(len(split_part(split_part(substr(DURATION,3),'M',1),'H',-1))>2 ,'00',split_part(split_part(substr(DURATION,3),'M',1),'H',-1))),':',
                        REPLACE(iff(substr(DURATION,3) NOT LIKE '%S%', '00',
                                    iff(substr(DURATION,3) LIKE '%H%',  split_part(split_part(substr(DURATION,3),'H',-1),'M',-1),
                                        split_part(substr(DURATION,3),'M',-1))),'S','')
                       )) END                                       AS DURATION_TIME,
  second(DURATION_TIME)+minute(DURATION_TIME)*60
    +hour(DURATION_TIME)*60*60                                      AS DURATION_IN_SECONDS,
  IS_SHORT                                                          AS YOUTUBE_IS_SHORT_VIDEO,
  concat('https://www.youtube.com/watch?v=',ED.VIDEO_ID)            AS REFERENCE_LINK,
  concat('https://www.youtube.com/channel/',CHANNEL_ID)             AS CHANNEL_LINK
  FROM      {{ source('WSC_ENV_DB', 'MRR_SOCIAL_ENGAGEMENT_YOUTUBE') }} ED 
  LEFT JOIN {{ source('WSC_ENV_DB', 'MRR_PUBLISHJOBUPLOADREFERENCES') }} PBL 
  ON LOWER(ED.video_id) = LOWER(split_part(PBL.value,'v=',-1)) AND PBL.VALUE LIKE '%youtube%'
    
UNION ALL
    
SELECT DISTINCT 
  PAGE_ID                                                           AS CHANNEL_ID,
  PAGE_TITLE                                                        AS CHANNEL_TITLE,
  NULL                                                              AS CHANNEL_DISPLAY_TITLE,
  'Facebook'                                                        AS PUBLISHAPP_NAME,
  ED.MEDIA_OBJECT_ID                                                AS REFERENCE_ID,
  'VIDEO'                                                           AS REFERENCE_TYPE,
  MEDIA_OBJECT_CAPTION                                              AS REFERENCE_TITLE,
  to_timestamp(replace(PUBLISHED_AT,'T',' '))                       AS PUBLISH_DATETIME,
  fb_reaction_data.total_reactions                                  AS LIKES,
  NULL                                                              AS COMMENTS,
  NULL                                                              AS ENGAGEMENT,
  VIDEO_VIEWS                                                       AS VIEWS,
  IMPRESSIONS                                                       AS IMPRESSIONS,
  REACH                                                             AS REACH,
  NULL                                                              AS RETWEETS,
  NULL                                                              AS HASHTAGS,
  PUBLISHJOBID                                                      AS PUBLISHJOBID,
  to_time(to_number(MEDIA_OBJECT_LENGTH)::VARCHAR)                  AS DURATION_TIME,
  to_number(MEDIA_OBJECT_LENGTH)                                    AS DURATION_IN_SECONDS,
  NULL                                                              AS YOUTUBE_IS_SHORT_VIDEO,
  concat('https://www.facebook.com/',PAGE_ID,'/videos/',ED.MEDIA_OBJECT_ID) AS REFERENCE_LINK,
  concat('https://www.facebook.com/',PAGE_ID)                       AS CHANNEL_LINK
  FROM      {{ source('WSC_ENV_DB', 'MRR_SOCIAL_ENGAGEMENT_FACEBOOK') }} ED 
  LEFT JOIN {{ source('WSC_ENV_DB', 'MRR_PUBLISHJOBUPLOADREFERENCES') }} PBL 
  ON LOWER(ED.MEDIA_OBJECT_ID) = LOWER(split_part(PBL.value,'posts/',-1)) AND PBL.VALUE LIKE '%facebook%'
  LEFT JOIN fb_reaction_data ON ED.media_object_id = fb_reaction_data.media_object_id