-- CLEAN UP
-----------

drop schema public cascade;
create schema public;


-- DIMENSIONS
-------------

create table member
(
  member_id serial primary key -- serial is implicit not null
  , is_active bool not null
  , year int not null
  , balance numeric not null
);

create table room
(
  room_id serial primary key
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

create table time
(
  time_id serial primary key
  , semester char(3) not null -- semester id
  , week int not null
  -- SHOULD WE ADD? , week_sem int not null -- semester week, starting at 1
  , day int not null
  , hour int not null
  , quarter_hour int not null -- 0..3 (one for each quarter of an hour)
  -- ATTRIBUTES
  , year int not null -- attr. for semester
  , is_spring bool not null -- attr. for semester
  , is_holiday bool not null -- attr. for day
  , is_weekend bool not null -- attr. for day
  , event_flan bool not null -- attr. for day
  , is_morning bool not null -- attr. for day
  , is_afternoon bool not null -- attr. for day
);

create table product
(
  product_id serial primary key
  , type product_type not null default 'undefined' -- could just be a text field
  --, price_range product_price_range not null -- (0: 0-10 1: 11-50, etc.) -- NEEDED?
  , name text not null
  , price numeric not null
  , is_active bool not null
  , deactivation_date int references time(time_id)
);


-- FACT TABLES
--------------

create table sale
(
  price numeric not null
  , time_id int not null references time(time_id)
  , product_id int not null references product(product_id)
  , room_id int not null references room(room_id)
  , member_id int not null references member(member_id)
);

