insert into member (is_active, year, balance) values
  (true, 2006, 10.25),
  (false, 2005, -100.56),
  (true, 1998, 100.867);

insert into room (name, description) values
  ('Websalg', 'Websalg'),
  ('cs', 'Jægerstuen'),
  ('math', 'Vektorrummet'),
  ('Fyttetur', 'Fyttetur'),
  ('Julefrokost', 'Julefrokost');

insert into product (type, name, price, is_active, deactivation_date) values
  ('snack', 'bbq chips', 10, true, null),
  ('alcohol', 'pilsner', 7.5, true, null),
  ('dairy', '1/4 l. mælk', 5, false, '2001-04-12 13:37');

insert into time (semester, week, day, hour, quarter_hour, year, is_spring, is_holiday, is_weekend, event_flan, is_morning, is_afternoon) values
  (1, 13, 1, 9, 2, 2002, false, false, false, false, true, false),
  (1, 9, 5, 17, 0, 2002, false, false, false, false, false, true),
  (2, 2, 2, 11, 3, 2002, true, true, false, false, true, false) ;

insert into sale (price, time_id, product_id, room_id, member_id) values
  ( 7.00, 2, 2, 2, 3),
  ( 5.00, 1, 3, 1, 2),
  (10.00, 1, 1, 1, 1),
  (10.00, 3, 1, 1, 2),
  ( 5.00, 2, 3, 3, 3),
  ( 7.00, 3, 2, 2, 1),
  ( 5.00, 1, 3, 2, 2),
  ( 3.50, 2, 2, 1, 1);

