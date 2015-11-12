drop schema public cascade;
create schema public;

CREATE TABLE member
(
  id smallint,
  active smallint,
  year smallint,
  balance integer
);

CREATE TABLE payment
(
  id integer,
  member_id smallint,
  "timestamp" timestamp without time zone,
  amount integer
);

CREATE TABLE product
(
  id smallint,
  name character varying,
  price integer,
  active smallint,
  deactivation_date timestamp without time zone
);

CREATE TABLE room
(
  id smallint,
  name character varying,
  description character varying
);

CREATE TABLE sale
(
  id integer,
  member_id smallint,
  product_id smallint,
  room_id smallint,
  "timestamp" timestamp without time zone,
  price integer
);

CREATE TABLE semestergroups
(
  member_id smallint,
  periode character varying,
  semester character varying
);
