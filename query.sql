Drop table if exists ##abc --To get final recorder time at each bus stop
Drop table if exists ##pqr --To get initial recorded time at each bus stop
Drop table if exists ##def
Drop table if exists ##xyz
Drop table if exists ##final


select f.gtfs_trip_id
	,f.vehicle_id
	,f.next_stop_id
	,f.recorded_at_time
	,f.next_predicted_arrival_time
	,rank() over(partition by next_stop_id,vehicle_id,gtfs_trip_id order by recorded_at_time desc) as seq
into ##abc
from
	(
	select lat
		,lon
		,route
		,gtfs_trip_id
		,origin_stop_id
		,destination_stop_id
		,progress_rate
		,vehicle_id
		,gtfs_block_id
		,next_stop_id
		,recorded_at_time
		--,CONVERT(varchar(15),recorded_at_time,100)
		,next_predicted_arrival_time
		,next_stop_distance_from_origin
		,next_stop_distance_to_vehicle
		,stops_from_call_presentable
		,at_stop_boolean
		--,RANK() over(order by next_stop_id) as seq 
	from bus_data
	where route ='MTABC_Q70+'
	--and 
	--gtfs_trip_id = 'MTABC_18522787-LGPE7-LG_E7-Sunday-70'
	--or
	--gtfs_trip_id = 'MTABC_18522765-LGPE7-LG_E7-Sunday-70'
	
	--order by recorded_at_time asc
	
	)f
group by f.next_stop_id
	,f.vehicle_id
	, f.recorded_at_time
	,f.next_predicted_arrival_time
	,f.gtfs_trip_id
order by f.recorded_at_time asc
--select * from ##abc
----here vehicle_id = 'MTABC_7451'
----and 
--where seq =1
--order by recorded_at_time



select 
	s.gtfs_trip_id
	,s.vehicle_id
	,s.next_stop_id
	,s.recorded_at_time
	,s.next_predicted_arrival_time
	,rank() over(partition by next_stop_id ,vehicle_id,gtfs_trip_id order by recorded_at_time asc) as seq
into ##pqr
from
	(
	select lat
		,lon
		,route
		,gtfs_trip_id
		,origin_stop_id
		,destination_stop_id
		,progress_rate
		,vehicle_id
		,gtfs_block_id
		,next_stop_id
		,recorded_at_time
		,next_predicted_arrival_time
		,next_stop_distance_from_origin
		,next_stop_distance_to_vehicle
		,stops_from_call_presentable
		,at_stop_boolean
		--,RANK() over(order by next_stop_id) as seq 
	from bus_data
	where route ='MTABC_Q70+'
	--and 
	--gtfs_trip_id = 'MTABC_18522787-LGPE7-LG_E7-Sunday-70'
	--or
	--gtfs_trip_id = 'MTABC_18522765-LGPE7-LG_E7-Sunday-70'
	--order by recorded_at_time asc
	)s
group by s.next_stop_id
	,s.vehicle_id
	, s.recorded_at_time
	,s.next_predicted_arrival_time
	,s.gtfs_trip_id
order by s.recorded_at_time asc


--select * from ##pqr
----here vehicle_id = 'MTABC_7451'
----and 
--where seq =1
--order by recorded_at_time


--select * from ##pqr
--order by recorded_at_time

select a.gtfs_trip_id 
	,a.vehicle_id
	,a.next_stop_id
	,a.recorded_at_time as recorded_time_end
	,a.next_predicted_arrival_time as predicted_arrival_time_end
	--,cast(a.recorded_at_time as time) [recorded_time_end] 
	--,cast(a.next_predicted_arrival_time as time) [predicted_arrival_time_end] 
into ##def
from ##abc a
where seq =1
group by a.gtfs_trip_id
	,a.vehicle_id
	,a.next_stop_id
	,a.recorded_at_time
	,a.next_predicted_arrival_time
	--,cast(a.recorded_at_time as time) 
	--,cast(a.next_predicted_arrival_time as time) 
	,recorded_at_time
order by recorded_at_time

--select * from ##def
--order by recorded_time_end

select p.gtfs_trip_id
	,p.vehicle_id
	,p.next_stop_id
	,p.recorded_at_time as recorded_time_initial
	,p.next_predicted_arrival_time as predicted_arrival_time_initial
	--,cast(p.recorded_at_time as time) [recorded_time_initial] 
	--,cast(p.next_predicted_arrival_time as time) [predicted_arrival_time_initial] 
into ##xyz
from ##pqr p
where seq =1
--and gtfs_trip_id = 'MTABC_18522766-LGPE7-LG_E7-Sunday-70'
group by p.gtfs_trip_id
	,p.vehicle_id
	,p.next_stop_id
	,p.recorded_at_time
	,p.next_predicted_arrival_time
	--,cast(p.recorded_at_time as time) 
	--,cast(p.next_predicted_arrival_time as time) 
	,recorded_at_time
order by recorded_at_time



--select * from ##xyz
--order by recorded_time_initial
--select * from ##def
--order by recorded_time_end


select d.gtfs_trip_id 
	,d.vehicle_id
	,d.next_stop_id
	,x.recorded_time_initial
	,x.predicted_arrival_time_initial
	,d.recorded_time_end
	,d.predicted_arrival_time_end
into ##final
from ##def d
inner join ##xyz x
on d.next_stop_id = x.next_stop_id
and  d.vehicle_id = x.vehicle_id
and d.gtfs_trip_id = x.gtfs_trip_id 
group by d.gtfs_trip_id 
	,d.vehicle_id
	,d.next_stop_id
	,x.recorded_time_initial
	,x.predicted_arrival_time_initial
	,d.recorded_time_end
	,d.predicted_arrival_time_end
order by recorded_time_initial
--select * from ##final
--order by recorded_time_initial
select f.*
	,DATEDIFF ( MINUTE,f.predicted_arrival_time_initial,f.predicted_arrival_time_end )   as 'delay'
	,DATEDIFF ( MINUTE,f.recorded_time_initial,f.predicted_arrival_time_end )   as 'travel_time'
from ##final f
--where f.gtfs_trip_id = 'MTABC_18522877-LGPE7-LG_E7-Sunday-70' 
order by recorded_time_initial
