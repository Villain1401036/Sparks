
--------------------------------------------------------------
      ---------------- Postgresql ---------------
--------------------------------------------------------------

-- How many total messages are being sent every day?
select createddate as date , count(*) from  (select Date(createdAt) as createddate from messages) a group by createddate 


-- Are there any users that did not receive any message?
select id from users where not exists (select distinct receiverId from messages )


-- How many active subscriptions do we have today?
select count(*) from subscriptions where status = 'Active'


--  Are there users sending messages without an active subscription? (some extra context for you: in our apps only premium users can send messages).
select user_id from subscriptions s inner join messages me on me.senderId = s.user_id  where status = 'Active' and me.createdAt not between s.startDate and EndDate  


-- How much is the average subscription amount (sum amount subscriptions /count subscriptions) breakdown by year/month (format YYYY-MM)?
select createdMonth , sum(amount)/count(*) as avgprice from (select id, amount , to_char(createdDate, 'YYYY-MM') as createdMonth from subscription where status <> 'Rejected' ) group by createdMonth order by createdMonth

--------------------------------------------------------------
      ---------------- Bigquery sql ---------------
--------------------------------------------------------------

 -- How many total messages are being sent every day?
select createddate as date , count(*) from  (select Date(createdAt) as createddate from sparks-363212.users.messages) a group by createddate ;

-- Are there any users that did not receive any message?
select id from sparks-363212.users.users where not exists (select distinct receiverId from sparks-363212.users.messages );


-- How many active subscriptions do we have today?
select count(*) from sparks-363212.users.subscriptions where status = 'Active';


--  Are there users sending messages without an active subscription? (some extra context for you: in our apps only premium users can send messages).
select s.user_id from sparks-363212.users.subscriptions s inner join sparks-363212.users.messages me on me.senderId = s.user_id  where status = 'Active' and me.createdAt not between s.startDate and EndDate;


-- How much is the average subscription amount (sum amount subscriptions /count subscriptions) breakdown by year/month (format YYYY-MM)?
select createdMonth , sum(amount)/count(*) as avgprice from (select user_id, amount , EXTRACT(month from createdAt) as createdMonth from sparks-363212.users.subscriptions where status <> 'Rejected' ) group by createdMonth order by createdMonth