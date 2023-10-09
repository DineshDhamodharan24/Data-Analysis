with probability as (
  WITH quantity as (
  SELECT productid,sum(quantity) as qty FROM `Order Details` 
  WHERE orderid in (SELECT orderid FROM Orders
  WHERE customerid in (SELECT customerid FROM Customers )) group by productid ORDER by productid
)  
  SELECT p.categoryid,sum(q.qty) as total_qty FROM quantity q
  inner join Products p on p.ProductID=q.productid
  group by p.categoryid
  order by p.categoryid
)
select c.categoryid,c.categoryname,c.description,p.total_qty from probability p
inner join Categories c on c.CategoryID = p.categoryid