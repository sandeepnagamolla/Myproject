insert into EMP_PROC.EMP_SAL_GREATER_MNGER
select e.ssn as ESSN, 
       e.salary as Salary, 
       ep.super_ssn as Super_ssn,
       ep.salary as Salary
       from EMP_RAW.employee e
       JOIN EMP_RAW.employee ep on e.super_ssn = ep.ssn
       where e.salary > ep.salary;



insert into emp_proc.emp_project_dept
SELECT 
    e.ssn AS essn, 
    p.pname, 
    ed.dname AS emp_dept_name, 
    pd.dname AS proj_dept_name
FROM 
    EMP_RAW.employee e
JOIN 
    EMP_RAW.works_on wo ON e.ssn = wo.essn
JOIN 
    EMP_RAW.project p ON wo.pno = p.pnumber
JOIN 
    EMP_RAW.department ed ON e.dno = ed.dnumber
JOIN 
    EMP_RAW.department pd ON p.dnum = pd.dnumber
WHERE 
    e.dno != p.dnum;


insert into emp_proc.emp_dept_least
select d.dname as dept_name,
       d.dnumber as dept_no,
       count(distinct e.ssn) as no_of_emp,  
from EMP_RAW.employee e
join EMP_RAW.department d on e.dno = d.dnumber
group by d.dname, 
         d.dnumber;



insert into emp_proc.emp_tot_hrs_spent
select w.essn, 
       dp.dependent_name, 
       w.pno, sum(w.hours)
from EMP_RAW.works_on w
join EMP_RAW.dependent dp on w.essn = dp.essn
group by w.essn, 
         dp.dependent_name, 
         w.pno
order by w.essn, 
         w.pno;


insert into emp_proc.emp_full_details
select e.ssn as essn,
       e.fname,
       e.minit,
       e.lname,
       e.bdate,
       e.address,
       e.sex,
       e.salary,
       e.super_ssn,
       e.dno,
       d.dname,
       dl.dlocation,
       p.pname,
       p.pnumber,
       p.plocation,
       w.hours as total_hours,
       dp.dependent_name,
       dp.sex,
       dp.relationship
from EMP_RAW.employee e
join EMP_RAW.department d on d.dnumber = e.dno
join EMP_RAW.dept_locations dl on dl.dnumber = d.dnumber
join EMP_RAW.project p on p.dnum = dl.dnumber
join EMP_RAW.works_on w on w.pno = p.pnumber
join EMP_RAW.department d1 on d1.dnumber = p.dnum
join EMP_RAW.dept_locations dl1 on dl1.dnumber = e.dno
join EMP_RAW.dependent dp on e.ssn = dp.essn;
