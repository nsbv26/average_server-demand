select a.name
,count(b.id)
,to_char(b.due_date,'YYYY-MM-01') as MONTH



from solutions a
JOIN assets b ON b.solution_id = a.id
JOIN sites c ON c.id = b.site_id
WHERE b.status not in (7) and c.name like '%CTC%'

group by MONTH,a.name
