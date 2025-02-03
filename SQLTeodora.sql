if object_id ('Customer','u') IS NOT NULL
drop table Customer;
if object_id ('Users','u') IS NOT NULL
drop table Users;
if object_id ('Products','u') IS NOT NULL
drop table Products;
if object_id ('Managers','u') IS NOT NULL
drop table Managers;
if object_id ('Orders','u') IS NOT NULL
drop table Orders;
if object_id ('Locations','u') IS NOT NULL
drop table Locations;
if object_id ('Region','u') IS NOT NULL
drop table Region;
if object_id ('category','u') IS NOT NULL
drop table category;
if object_id ('Subcategory','u') IS NOT NULL
drop table Subcategory;
if object_id ('Returned','u') IS NOT NULL
drop table Returned;


create table Managers (
RowID INT PRIMARY KEY IDENTITY(1,1),
Manager varchar(30) not null,
)


insert into Managers(Manager) 
values ('Chris'),('Erin'),('Sam'),('William');

select * from Managers


create table Region (
RowID INT PRIMARY KEY IDENTITY(1,1),
Region varchar(30) not null CONSTRAINT UQ_Region UNIQUE,
);

insert into Region (Region) 
values ('Central'),('East'),('South'),('West');

select * from Region

create table Locations (
PostalCode numeric(5,0) primary key,
Country varchar(30) not null,
StateOrProvince varchar(30)  not null,
City varchar(30)  not null,
);
select * from Locations


create table Users (
RowID INT PRIMARY KEY IDENTITY(1,1),
id_manager int,
id_region int,
constraint fk_userman foreign key (id_manager) references Managers(RowID),
constraint fk_userreg foreign key (id_region) references Region(RowID),
)

insert into Users(id_manager, id_region) 
values (1,1),(2,2),(3,3),(4,4);

select * from Users

create table Customer (
RowID INT PRIMARY KEY ,
CustomerName varchar(30) not null, 
Segment VARCHAR(30) NOT NULL CHECK (Segment IN ('Consumer', 'Corporate','Home Office','Small Business'))
);
select * from Customer

create table Category (
RowID INT PRIMARY KEY IDENTITY(1,1),
Category varchar(30) not null
)
insert into Category (Category) 
values ('Furniture'), ('Office Supplies'), ('Technology');

select * from Category


create table Subcategory (
RowID INT PRIMARY KEY IDENTITY(1,1),
Category int not null,
Subcategory varchar(30) not null,
CONSTRAINT FK_Subcat FOREIGN KEY (category) REFERENCES Category (RowID),
)

INSERT INTO Subcategory (Category, Subcategory) 
VALUES 
(1, 'Office Furnishings'),
(1, 'Chairs & Chairmats'),
(1, 'Bookcases'),
(1, 'Tables'),
(2, 'Paper'),
(2, 'Rubber Bands'),
(2, 'Envelopes'),
(2, 'Scissors, Rulers and Trimmers'),
(2, 'Binders and Binder Accessories'),
(2, 'Labels'),
(2, 'Storage & Organization'),
(3, 'Computer Peripherals'),
(3, 'Telephones and Communication'),
(3, 'Office Machines'),
(3, 'Copiers and Fax'),
(2, 'Appliances'),
(2, 'Pens & Art Supplies');

select * from Subcategory

create table Products (
RowID INT PRIMARY KEY,
ProductName varchar(100) not null, 
ProductContainer VARCHAR(30) NOT NULL CHECK (ProductContainer IN ('Jumbo Box', 'Jumbo Drum','Large Box','Medium Box','Small Box','Small Pack','Wrap Bag')),
Subcategory int not null,
CONSTRAINT FK_Product FOREIGN KEY (subcategory) REFERENCES Subcategory(RowId),
)

select distinct ProductName from Products

create table Orders(
rowID int primary key,
OrderID INT not null,
id_customer int not null,
id_product int not null,
id_region int not null,
id_location numeric(5,0) not null,

product_base_margin numeric(10,2) null,
unit_price numeric(10,2) not null ,

returned varchar(20)check (returned in ('Returned') or returned is null),
shipping_cost numeric(10,2) not null,
ship_mode varchar(30) not null check (ship_mode in ('Delivery Truck','Express Air','Regular Air')),
shipping_date Date not null,

discount numeric(10,2) not null,
order_priority VARCHAR(30) NOT NULL CHECK (order_priority IN ('Critical','High','Low','Not specified','Medium')),
profit numeric(10,2) not null,
quantity numeric(10) not null,
sales numeric(10, 2) not null,
order_date Date not null,


CONSTRAINT FK_Customer FOREIGN KEY (id_customer) REFERENCES Customer(RowID),
CONSTRAINT FK_ProductId FOREIGN KEY (id_product) REFERENCES Products(RowID),
CONSTRAINT FK_Region Foreign key (id_region) references Region(RowID),
CONSTRAINT FK_Address Foreign key (id_location) references Locations(PostalCode),
CONSTRAINT chk_sc3 CHECK (shipping_cost >= 0),
CONSTRAINT chk_d3 CHECK (discount >= 0 AND discount <= 100),
CONSTRAINT chk_q3 CHECK (quantity >= 0),

CONSTRAINT chk_s3 CHECK (sales >= 0),

CONSTRAINT chk_dr3 CHECK (
        shipping_cost = ROUND(shipping_cost, 2) AND
        discount = ROUND(discount, 2) AND
        profit = ROUND(profit, 2) AND
        sales = ROUND(sales, 2) and
		product_base_margin =ROUND(product_base_margin, 2) and
		unit_price = ROUND(unit_price, 2)
	
    )

)

select * from Orders


----------------------------