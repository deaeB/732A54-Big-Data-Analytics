select * from jbemployee;
select name from jbdept order by name;
select * from jbparts where qoh = 0;
select * from jbemployee where salary between 9000 and 10000;
select *,startyear - birthyear age from jbemployee;
select * from jbemployee where name like '%son,%';
select * from jbitem where supplier = (select id from jbsupplier where name = 'Fisher-Price');
select i.* from jbitem i, jbsupplier s where (i.supplier = s.id && s.name = 'Fisher-Price');
select * from jbcity where id in (select city from jbsupplier);
select name, color from jbparts where weight > (select weight from jbparts where name = 'card reader');
select p1.name, p1.color from jbparts p1 join jbparts p2 on p1.weight > p2.weight where p2.name = 'card reader';
select avg(weight) from jbparts;
select s.name,sum(s.quan * p.weight) as 'total weight' from (
	select * from jbsupply s join (
		select id,name from jbsupplier where city in (
			select id from jbcity where state = 'mass'
		)
	)as slr on s.supplier = slr.id
)as s join jbparts as p on s.part = p.id  group by supplier
;

create table item(
    id int unsigned,
    name varchar(20),
    dept int,
    price int unsigned,
    qoh int unsigned,
    supplier int,
    constraint pk_item primary key(id),
	constraint fk_deliver_supplier foreign key (supplier) references jbsupplier(id),
    constraint fk_onshelf_dept foreign key (dept) references jbdept(id)
);
select * from jbitem where price < (select avg(price) from jbitem);
insert into item values
   ( 11, 'Wash Cloth',         1,   75,  575, 213),
   ( 19, 'Bellbottoms',       43,  450,  600,  33),
   ( 21, 'ABC Blocks',         1,  198,  405, 125),
   ( 23, '1 lb Box',          10,  215,  100,  42),
   ( 25, '2 lb Box, Mix',     10,  450,   75,  42),
   ( 26, 'Earrings',          14, 1000,   20, 199),
   ( 43, 'Maze',              49,  325,  200,  89),
   (106, 'Clock Book',        49,  198,  150, 125),
   (107, 'The ''Feel'' Book', 35,  225,  225,  89),
   (118, 'Towels, Bath',      26,  250, 1000, 213),
   (119, 'Squeeze Ball',      49,  250,  400,  89),
   (120, 'Twin Sheet',        26,  800,  750, 213),
   (165, 'Jean',              65,  825,  500,  33),
   (258, 'Shirt',             58,  650, 1200,  33)
;
select * from item;