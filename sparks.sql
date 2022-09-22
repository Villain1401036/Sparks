
---creating types 

DO $$ BEGIN
    CREATE or REPLACE TYPE gen AS ENUM ('male','female','trans');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


DO $$ BEGIN
    CREATE TYPE sub_status AS ENUM ('Active','Rejected');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


---creating tables

DO $$ BEGIN
    create table users(
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
)

EXCEPTION
    WHEN duplicate_object THEN null;
END $$;



DO $$ BEGIN
    create table messages (
	id text, 
	createdAt timestamp not null, 
   message text ,
   receiverId text not null, 
   senderId text not null ,
	Foreign key (receiverId) references users(id),
	Foreign key (senderId) references users(id)
)

EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

create table subscriptions (
	subs_id bigserial Primary key,
	user_id text not null ,
	createdDate timestamp not null, 
	startDate  timestamp not null, 
	endDate timestamp , 
	status sub_status,
	amount float,
	Foreign key (user_id) references users(id)
)


DO $$ BEGIN
    create table messages (
	id text, 
	createdAt timestamp not null, 
   message text ,
   receiverId text not null, 
   senderId text not null ,
	Foreign key (receiverId) references users(id),
	Foreign key (senderId) references users(id)
)

EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

---creating indexes

DO $$ BEGIN
    create index user_place_idx on user(city ,country); 
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;



