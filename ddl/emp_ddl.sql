create table if not exists emp_raw.employee (
	fname varchar(30),
	minit char(1),
	lname varchar(30),
	ssn char(9),
	bdate date,
	address varchar(30),
	sex char(1),
	salary decimal(10,2),
	super_ssn char(9),
	dno int
);
create table if not exists emp_raw.department(
	dname varchar(30),
	dnumber smallint,
	mgr_ssn char(9),
	mgr_start_date date
);
create table if not exists emp_raw.dependent(
	essn char(9),
	dependent_name varchar(30),
	sex char(1),
	bdate date,
	relationship varchar(20)
);
create table if not exists emp_raw.dept_locations(
	dnumber smallint,
	dlocation varchar(20)
);

create table if not exists emp_raw.project(
	pname varchar(30),
	pnumber smallint,
	plocation varchar(30),
	dnum smallint
);

create table if not exists works_on(
	essn char(9),
	pno smallint,
	hours decimal(4,2)
);


create table if not exists emp_proc.emp_sal_greater_mnger(
Essn char(9),
Salary decimal(10,2),
super_ssn char(9),
SSalary decimal(10,2)
);

create table if not exists emp_proc.emp_project_dept(
essn char(9),
pname varchar(30),
emp_dept_name varchar(30),
proj_dept_name varchar(30)
);

create table if not exists emp_proc.emp_tot_hrs_spent(
essn char(9),
department_name varchar(30),
pno smallint,
total_hours_spent int
);

create table if not exists emp_proc.emp_full_details(
essn char(9),
fname varchar(30),
minit char(1),
lname varchar(30),
bdate date,
address varchar(30),
sex char(1),
salary decimal(10,2),
super_ssn char(9),
dno int,
dname varchar(30),
dlocation varchar(20),
pname varchar(30),
pnumber smallint,
plocation varchar(30),
hours decimal(4,2),
dependent_name varchar(30),
dependent_sex char(1),
relationship varchar(20)
);

create table if not exists emp_proc.emp_dept_least(
dname varchar(30),
dno int,
no_of _emp int
);
