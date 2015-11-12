-- CLEAN UP
-----------

drop schema public cascade;
create schema public;


-- DIMENSIONS
-------------

create table member
(
  id serial primary key -- serial is implicit not null
  , is_active bool not null
  , year smallint not null
  , balance money not null
);

create table room
(
  id serial primary key
  , name text not null
  , description text not null
);

create type product_type as enum
(
  'undefined'
  , 'snack'
  , 'alcohol'
  , 'dairy'
  -- etc...
);

create table product
(
  id serial primary key
  , type product_type not null default 'undefined' -- could just be a text field
  --, price_range product_price_range not null -- (0: 0-10 1: 11-50, etc.) -- NEEDED?
  , name text not null
  , price money not null
  , is_active bool not null
  , deactivation_date date --not null -- should this be a foreign key to time dimension?
);

create table time
(
  id serial primary key
  , semester int not null -- semester id
  , week smallint not null
  , day smallint not null
  , hour smallint not null
  , quarter_hour smallint not null -- 0..3 (one for each quarter of an hour)
  -- ATTRIBUTES
  , year smallint not null -- attr. for semester
  , is_spring bool not null -- attr. for semester
  , is_holiday bool not null -- attr. for day
  , is_weekend bool not null -- attr. for day
  , event_flan bool not null -- attr. for day
  , is_morning bool not null -- attr. for day
  , is_afternoon bool not null -- attr. for day
);


-- FACT TABLES
--------------

create table sale
(
  price money not null
  , time_id int not null references time(id)
  , product_id int not null references product(id)
  , room_id int not null references room(id)
  , member_id int not null references member(id)
);

