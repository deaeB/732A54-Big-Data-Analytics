# Big Data Lab01

1) List all employees, i.e. all tuples in the jbemployee relation.

`select * from jbemployee;`

| id   | name               | salary | manager | birthyear | startyear |
| ---- | ------------------ | ------ | ------- | --------- | --------- |
| 10   | Ross, Stanley      | 15908  | 199     | 1927      | 1945      |
| 11   | Ross, Stuart       | 12067  |         | 1931      | 1932      |
| 13   | Edwards, Peter     | 9000   | 199     | 1928      | 1958      |
| 26   | Thompson, Bob      | 13000  | 199     | 1930      | 1970      |
| 32   | Smythe, Carol      | 9050   | 199     | 1929      | 1967      |
| 33   | Hayes, Evelyn      | 10100  | 199     | 1931      | 1963      |
| 35   | Evans, Michael     | 5000   | 32      | 1952      | 1974      |
| 37   | Raveen, Lemont     | 11985  | 26      | 1950      | 1974      |
| 55   | James, Mary        | 12000  | 199     | 1920      | 1969      |
| 98   | Williams, Judy     | 9000   | 199     | 1935      | 1969      |
| 129  | Thomas, Tom        | 10000  | 199     | 1941      | 1962      |
| 157  | Jones, Tim         | 12000  | 199     | 1940      | 1960      |
| 199  | Bullock, J.D.      | 27000  |         | 1920      | 1920      |
| 215  | Collins, Joanne    | 7000   | 10      | 1950      | 1971      |
| 430  | Brunet, Paul C.    | 17674  | 129     | 1938      | 1959      |
| 843  | Schmidt, Herman    | 11204  | 26      | 1936      | 1956      |
| 994  | Iwano, Masahiro    | 15641  | 129     | 1944      | 1970      |
| 1110 | Smith, Paul        | 6000   | 33      | 1952      | 1973      |
| 1330 | Onstad, Richard    | 8779   | 13      | 1952      | 1971      |
| 1523 | Zugnoni, Arthur A. | 19868  | 129     | 1928      | 1949      |
| 1639 | Choy, Wanda        | 11160  | 55      | 1947      | 1970      |
| 2398 | Wallace, Maggie J. | 7880   | 26      | 1940      | 1959      |
| 4901 | Bailey, Chas M.    | 8377   | 32      | 1956      | 1975      |
| 5119 | Bono, Sonny        | 13621  | 55      | 1939      | 1963      |
| 5219 | Schwarz, Jason B.  | 13374  | 33      | 1944      | 1959      |
|      |                    |        |         |           |           |


2) List the name of all departments in alphabetical order. Note: by “name” we mean
the name attribute for all tuples in the jbdept relation.

`select name from jbdept order by name;`


| name             |
| ---------------- |
| Bargain          |
| Book             |
| Candy            |
| Children's       |
| Children's       |
| Furniture        |
| Giftwrap         |
| Jewelry          |
| Junior Miss      |
| Junior's         |
| Linens           |
| Major Appliances |
| Men's            |
| Sportswear       |
| Stationary       |
| Toys             |
| Women's          |
| Women's          |
| Women's          |


3) What parts are not in store, i.e. qoh = 0? (qoh = Quantity On Hand)


`select * from jbparts where qoh = 0;`

| id  | name              | color | weight | qoh |
| --- | ----------------- | ----- | ------ | --- |
| 11  | card reader       | gray  | 327    | 0   |
| 12  | card punch        | gray  | 427    | 0   |
| 13  | paper tape reader | black | 107    | 0   |
| 14  | paper tape punch  | black | 147    | 0   |


4) Which employees have a salary between 9000 (included) and 10000 (included)?


`select * from jbemployee where salary between 9000 and 10000;`


| id  | name           | salary | manager | birthyear | startyear |
| --- | -------------- | ------ | ------- | --------- | --------- |
| 13  | Edwards, Peter | 9000   | 199     | 1928      | 1958      |
| 32  | Smythe, Carol  | 9050   | 199     | 1929      | 1967      |
| 98  | Williams, Judy | 9000   | 199     | 1935      | 1969      |
| 129 | Thomas, Tom    | 10000  | 199     | 1941      | 1962      |


5) What was the age of each employee when they started working (startyear)?

`select *,startyear - birthyear age from jbemployee;`

| id   | name                | salary | manager | birthyear | startyear | age |
| ---- | ------------------- | ------ | ------- | --------- | --------- | --- |
| 10   | Ross, Stanley       | 15908  | 199     | 1927      | 1945      | 18  |
| 11   | Ross, Stuart        | 12067  |         | 1931      | 1932      | 1   |
| 13   | Edwards, Peter      | 9000   | 199     | 1928      | 1958      | 30  |
| 26   | Thompson, Bob       | 13000  | 199     | 1930      | 1970      | 40  |
| 32   | Smythe, Carol       | 9050   | 199     | 1929      | 1967      | 38  |
| 33   | Hayes, Evelyn       | 10100  | 199     | 1931      | 1963      | 32  |
| 35   | Evans, Michael      | 5000   | 32      | 1952      | 1974      | 22  |
| 37   | Raveen, Lemont      | 11985  | 26      | 1950      | 1974      | 24  |
| 55   | James, Mary         | 12000  | 199     | 1920      | 1969      | 49  |
| 98   | Williams, Judy      | 9000   | 199     | 1935      | 1969      | 34  |
| 129  | Thomas, Tom         | 10000  | 199     | 1941      | 1962      | 21  |
| 157  | Jones, Tim          | 12000  | 199     | 1940      | 1960      | 20  |
| 199  | Bullock, J\.D\.     | 27000  |         | 1920      | 1920      | 0   |
| 215  | Collins, Joanne     | 7000   | 10      | 1950      | 1971      | 21  |
| 430  | Brunet, Paul C\.    | 17674  | 129     | 1938      | 1959      | 21  |
| 843  | Schmidt, Herman     | 11204  | 26      | 1936      | 1956      | 20  |
| 994  | Iwano, Masahiro     | 15641  | 129     | 1944      | 1970      | 26  |
| 1110 | Smith, Paul         | 6000   | 33      | 1952      | 1973      | 21  |
| 1330 | Onstad, Richard     | 8779   | 13      | 1952      | 1971      | 19  |
| 1523 | Zugnoni, Arthur A\. | 19868  | 129     | 1928      | 1949      | 21  |
| 1639 | Choy, Wanda         | 11160  | 55      | 1947      | 1970      | 23  |
| 2398 | Wallace, Maggie J\. | 7880   | 26      | 1940      | 1959      | 19  |
| 4901 | Bailey, Chas M\.    | 8377   | 32      | 1956      | 1975      | 19  |
| 5119 | Bono, Sonny         | 13621  | 55      | 1939      | 1963      | 24  |
| 5219 | Schwarz, Jason B\.  | 13374  | 33      | 1944      | 1959      | 15  |


6) Which employees have a last name ending with “son”?


`select * from jbemployee where name like '%son,%';`


| id  | name          | salary | manager | birthyear | startyear |
| --- | ------------- | ------ | ------- | --------- | --------- |
| 26  | Thompson, Bob | 13000  | 199     | 1930      | 1970      |


7) Which items (note items, not parts) have been delivered by a supplier called
Fisher-Price? Formulate this query using a subquery in the where-clause.


`select * from jbitem where supplier = (select id from jbsupplier where name = 'Fisher-Price');`


| id  | name            | dept | price | qoh | supplier |
| --- | --------------- | ---- | ----- | --- | -------- |
| 43  | Maze            | 49   | 325   | 200 | 89       |
| 107 | The 'Feel' Book | 35   | 225   | 225 | 89       |
| 119 | Squeeze Ball    | 49   | 250   | 400 | 89       |


8) Formulate the same query as above, but without a subquery.


`select i.* from jbitem i, jbsupplier s where (i.supplier = s.id && s.name = 'Fisher-Price');`


| id  | name            | dept | price | qoh | supplier |
| --- | --------------- | ---- | ----- | --- | -------- |
| 43  | Maze            | 49   | 325   | 200 | 89       |
| 107 | The 'Feel' Book | 35   | 225   | 225 | 89       |
| 119 | Squeeze Ball    | 49   | 250   | 400 | 89       |


9) Show all cities that have suppliers located in them. Formulate this query using a
subquery in the where-clause.


`select * from jbcity where id in (select city from jbsupplier);`


| id  | name           | state |
| --- | -------------- | ----- |
| 10  | Amherst        | Mass  |
| 21  | Boston         | Mass  |
| 100 | New York       | NY    |
| 106 | White Plains   | Neb   |
| 118 | Hickville      | Okla  |
| 303 | Atlanta        | Ga    |
| 537 | Madison        | Wisc  |
| 609 | Paxton         | Ill   |
| 752 | Dallas         | Tex   |
| 802 | Denver         | Colo  |
| 841 | Salt Lake City | Utah  |
| 900 | Los Angeles    | Calif |
| 921 | San Diego      | Calif |
| 941 | San Francisco  | Calif |
| 981 | Seattle        | Wash  |


10) What is the name and color of the parts that are heavier than a card reader?
Formulate this query using a subquery in the where-clause. (The SQL query must
not contain the weight as a constant.)


`select name, color from jbparts where weight > (select weight from jbparts where name = 'card reader');`


| name         | color  |
| ------------ | ------ |
| disk drive   | black  |
| tape drive   | black  |
| line printer | yellow |
| card punch   | gray   |


11) Formulate the same query as above, but without a subquery. (The query must not
contain the weight as a constant.)


`select p1.name, p1.color from jbparts p1 join jbparts p2 on p1.weight > p2.weight where p2.name = 'card reader';`


| name         | color  |
| ------------ | ------ |
| disk drive   | black  |
| tape drive   | black  |
| line printer | yellow |
| card punch   | gray   |


12) What is the average weight of black parts?


`select avg(weight) from jbparts where color = 'black';`


| avg(weight) |
| ----------- |
| 347.2500    |


13) What is the total weight of all parts that each supplier in Massachusetts (“Mass”)
has delivered? Retrieve the name and the total weight for each of these suppliers.
Do not forget to take the quantity of delivered parts into account. Note that one
row should be returned for each supplier

`
select s.name,sum(s.quan * p.weight) as 'total weight' from (
	select * from jbsupply s join (
		select id,name from jbsupplier where city in (
			select id from jbcity where state = 'mass'
		) 
	)as slr on  s.supplier = slr.id
)as s join jbparts as p on s.part = p.id  group by supplier
;
`


| name          | total weight |
|---------------|--------------|
| Fisher\-Price | 1135000      |
| DEC           | 3120         |


14)  Create a new relation (a table), with the same attributes as the table items using
the CREATE TABLE syntax where you define every attribute explicitly (i.e. not
as a copy of another table). Then fill the table with all items that cost less than the
average price for items. Remember to define primary and foreign keys in your
table!


`create table item(
    id int unsigned,
    name varchar(20),
    dept int,
    price int unsigned,
    qoh int unsigned,
    supplier int,
    constraint pk_item primary key(id),
	constraint fk_deliver_supplier foreign key (supplier) references jbsupplier(id),
    constraint fk_onshelf_dept foreign key (dept) references jbdept(id)
);`

`select * from jbitem where price < (select avg(price) from jbitem);`

| id  | name            | dept | price | qoh  | supplier |
|-----|-----------------|------|-------|------|----------|
| 11  | Wash Cloth      | 1    | 75    | 575  | 213      |
| 19  | Bellbottoms     | 43   | 450   | 600  | 33       |
| 21  | ABC Blocks      | 1    | 198   | 405  | 125      |
| 23  | 1 lb Box        | 10   | 215   | 100  | 42       |
| 25  | 2 lb Box, Mix   | 10   | 450   | 75   | 42       |
| 26  | Earrings        | 14   | 1000  | 20   | 199      |
| 43  | Maze            | 49   | 325   | 200  | 89       |
| 106 | Clock Book      | 49   | 198   | 150  | 125      |
| 107 | The 'Feel' Book | 35   | 225   | 225  | 89       |
| 118 | Towels, Bath    | 26   | 250   | 1000 | 213      |
| 119 | Squeeze Ball    | 49   | 250   | 400  | 89       |
| 120 | Twin Sheet      | 26   | 800   | 750  | 213      |
| 165 | Jean            | 65   | 825   | 500  | 33       |
| 258 | Shirt           | 58   | 650   | 1200 | 33       |


`
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
;`

`select * from item;`

| id  | name            | dept | price | qoh  | supplier |
|-----|-----------------|------|-------|------|----------|
| 11  | Wash Cloth      | 1    | 75    | 575  | 213      |
| 19  | Bellbottoms     | 43   | 450   | 600  | 33       |
| 21  | ABC Blocks      | 1    | 198   | 405  | 125      |
| 23  | 1 lb Box        | 10   | 215   | 100  | 42       |
| 25  | 2 lb Box, Mix   | 10   | 450   | 75   | 42       |
| 26  | Earrings        | 14   | 1000  | 20   | 199      |
| 43  | Maze            | 49   | 325   | 200  | 89       |
| 106 | Clock Book      | 49   | 198   | 150  | 125      |
| 107 | The 'Feel' Book | 35   | 225   | 225  | 89       |
| 118 | Towels, Bath    | 26   | 250   | 1000 | 213      |
| 119 | Squeeze Ball    | 49   | 250   | 400  | 89       |
| 120 | Twin Sheet      | 26   | 800   | 750  | 213      |
| 165 | Jean            | 65   | 825   | 500  | 33       |
| 258 | Shirt           | 58   | 650   | 1200 | 33       |
