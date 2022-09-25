-- change password for postgres

alter user postgres PASSWORD 'postgres';

---creating types 

DO $$ BEGIN
    CREATE TYPE gen AS ENUM ('male','female','trans');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


DO $$ BEGIN
    CREATE TYPE sub_status AS ENUM ('Active','Rejected');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


---creating tables


 create table IF NOT EXISTS users(
	id text Primary Key,
	createdAt timestamp not null, 
    updatedAt timestamp not null,
    firstName text not null,
    lastName text ,
    address text , 
    city	text,
    country text,
    zipCode text,
    email text not null unique,
   birthDate timestamp,
    gender gen,
   isSmoking boolean,
   profession text,
   income float
);


create table IF NOT EXISTS messages (
	id text, 
	createdAt timestamp not null, 
   message text ,
   receiverId text not null, 
   senderId text not null ,
	Foreign key (receiverId) references users(id),
	Foreign key (senderId) references users(id)
);

create table if not exists subscriptions (
	subs_id bigserial Primary key,
	user_id text not null ,
	createdDate timestamp not null, 
	startDate  timestamp not null, 
	endDate timestamp , 
	status sub_status,
	amount float,
	Foreign key (user_id) references users(id)
);

---creating indexes
create index IF NOT EXISTS user_place_idx on users(city ,country);



---create roles and privilages for PII 

create user test_user PASSWORD 'test_user';

create role bi_analysts ;

REVOKE Usage on SCHEMA public from bi_analysts;
GRANT bi_analysts TO test_user;

create schema EDW;
GRANT USAGE ON SCHEMA edw TO bi_analysts;
create view edw.users as select id ,createdAt, birthdate , city, country, email , gender, issmoking , income from users;
create view edw.subscriptions as select *  from subscriptions;
create view edw.messages as select id , senderid , createdat from messages ;

grant SELECT on edw.users to bi_analysts;
grant SELECT on edw.subscriptions to bi_analysts ;
grant SELECT on edw.messages to bi_analysts ;


