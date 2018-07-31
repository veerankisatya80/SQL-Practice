CREATE TABLE PHONE_LOG(SOURCE_PHONE_NUMBER INT,DESTINATION_PHONE_NUMBER INT,CALL_START_DATETIME DATETIME)
GO
 
INSERT INTO PHONE_LOG VALUES(1234,4567,'1/7/2011')
INSERT INTO PHONE_LOG VALUES(1234,2345,'1/7/2011')
INSERT INTO PHONE_LOG VALUES(1234,3456,'1/7/2011')
INSERT INTO PHONE_LOG VALUES(1234,3456,'1/7/2011')
INSERT INTO PHONE_LOG VALUES(1234,4567,'1/7/2011')
INSERT INTO PHONE_LOG VALUES(1222,7890,'1/7/2011')
INSERT INTO PHONE_LOG VALUES(1222,7680,'1/7/2011')
INSERT INTO PHONE_LOG VALUES(1222,2345,'1/7/2011')
 
Please provide an SQL query to display the source_phone_number and a flag where the flag needs to be set to Y if first called number and last called number are the same and N if the first called number
and last called number are different

 
Please provide an SQL query to display the source_phone_number and a flag where the flag needs to be set to Y if first called number and last called number are the same and N if the first called number
and last called number are different
select source_phone_number,
case when destination_phone_number = last_call
then 1 else 0 end as flag 
from
(
select source_phone_number,destination_phone_number,
lead(destination_phone_number) over(partition by source_phone_number order by source_phone_number ) last_call

from
(
select source_phone_number,destination_phone_number,next_call,rownum
from 
(
select source_phone_number,destination_phone_number,
row_number() over(partition by source_phone_number order by source_phone_number) as rownum,
lead(destination_phone_number) over(partition by source_phone_number order by source_phone_number ) next_call
  from phone_log 
  ) X 
 where rownum =1
 or next_call is null
 
   ) Y
  ) Z
  where last_call is not null
   