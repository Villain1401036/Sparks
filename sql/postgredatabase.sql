
---creating types 

CREATE TYPE gen AS ENUM ('male','female','trans')

CREATE TYPE sub_status AS ENUM ('Active','Rejected')

---creating tables
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


create table messages (
	id text, 
	createdAt timestamp not null, 
   message text ,
   receiverId text not null, 
   senderId text not null ,
	Foreign key (receiverId) references users(id),
	Foreign key (senderId) references users(id)
)

---creating indexes

create index user_place_idx on user(city ,country); 

create par