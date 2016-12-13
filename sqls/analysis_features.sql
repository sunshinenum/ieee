use ieee;
select * from all_features where u_id = '01261fbd670dffc1387f417a0cce50f5';
select * from full_table where q_id = 'e1a1009e93ea09bdd981029b592d89b8';

select u_id, count(u_id) as c 
from full_table 
where u_labels like '%18%' and label = '1'
group by u_id
;

-- does predict user all in train ?
select * from (
select distinct a.u_id as ad, nvl(b.u_id, 0) as bd from invited_without_label a 
left outer join train b on a.u_id = b.u_id
) t where bd = 0 ;
-- 810 / 28763

-- how many questions those users have not answered question ?
select count(*) from (
select a.*, b.bd from invited_without_label a
left outer join (
select * from (
select distinct a.u_id as ad, nvl(b.u_id, 0) as bd from invited_without_label a 
left outer join train b on a.u_id = b.u_id
) t ) b on a.u_id = b.ad 
) tt where bd = 0
;
-- 856
