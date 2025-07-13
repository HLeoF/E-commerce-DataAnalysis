# SQL Code
## Here offering the SQL to process raw dataset and create and user behavior models:

```
#创建数据库
create database taobao;
use taobao;

#创建表
create table user_behavior (user_id int(9), item_id int(9), category_id int(9), behavior_type varchar(5), timestamps int(14) );

select * from user_behavior limit 10;

### 数据预处理 ###

#检查空值
select * from user_behavior where user_id is null;
select * from user_behavior where item_id is null;
select * from user_behavior where category_id is null;
select * from user_behavior where behavior_type is null;
select * from user_behavior where timestamps is null;

#检查重复值
select user_id, item_id, timestamps from user_behavior group by user_id, item_id, timestamps having count(*) > 1;

#去重
alter table user_behavior add id int first; #添加一个辅助字段
alter table user_behavior modify id int primary key auto_increment;# 新增 id 字段 自增
select * from user_behavior limit 5;

delete user_behavior from user_behavior,(
    select user_id, item_id, timestamps, min(id) as id from user_behavior group by user_id, item_id, timestamps having count(*) > 1
) t2
where user_behavior.user_id = t2.user_id and
      user_behavior.item_id = t2.item_id and
      user_behavior.timestamps = t2.timestamps and
      user_behavior.id > t2.item_id;

#添加日期
#更改buffer值
show VARIABLES like '_buffer%';
set GLOBAL innodb_buffer_pool_size =1070000000;

#添加datatime
alter table user_behavior add datetimes TIMESTAMP(0);
update user_behavior set datetimes=from_unixtime(timestamps);
select * from user_behavior limit 5;

#将datetimes进行 细加工 加三个字段
alter table user_behavior add dates char(10);
alter table user_behavior add times char(8);
alter table user_behavior add hours char(2);


update user_behavior set dates = substring(datetimes,1,10);
update user_behavior set times = substring(datetimes,12,8);
update user_behavior set hours = substring(datetimes,12,2);
select * from user_behavior limit 5;

#清理异常值
select max(datetimes), min(datetimes) from user_behavior;
delete from user_behavior where datetimes < '2017-11-25 00:00:00' or datetimes > '2017-12-03 23:59:59';

select * from user_behavior where dates is null;
delete from user_behavior where dates is null;


### 获客分析 ##
create table acq_user (
    dates char(10),
    pv int(9),
    uv int(9),
    puv decimal(10,1)
);

insert into acq_user
select dates, count(*) 'pv', #获取pv量
       count(distinct user_id) 'uv', #获取uv量
       round(count(*)/count(distinct user_id),1) 'pv/uv' #获取pv/uv率
from user_behavior where behavior_type = 'pv' group by dates;

select * from acq_user;



### 留存分析 ###

#留存率
create table retention_rate(
    dates char(10),
    retention_1 float
);

insert into retention_rate
select a.dates,
       count(if(datediff(b.dates,a.dates)=1,b.user_id,null))/count(if(datediff(b.dates,a.dates)=0,b.user_id,null)) retention_1
from
    (select user_id, dates from user_behavior group by user_id, dates) a,
    (select user_id, dates from user_behavior group by user_id, dates) b
where a.user_id = b.user_id and a.dates<=b.dates
group by a.dates;

select * from retention_rate;

# 跳失率-用户只浏览了一次 -- 88个
# 跳转率 = 88/89660650

select a.cnt/(select sum(pv) sum_pv from acq_user) from (
    select count(user_id) cnt from user_behavior group by user_id having count(behavior_type) = 1
) a;

###行为分析
#时序分析  by hours

create table hourstimes_behav(
    date char(10),
    hours char(2),
    pv int,
    cart int,
    fav int,
    buy int);
insert into hourstimes_behav
select dates, hours ,
       count(if(behavior_type='pv', behavior_type, null)) 'pv',
       count(if(behavior_type='cart', behavior_type, null)) 'cart',
       count(if(behavior_type='fav', behavior_type, null)) 'fav',
       count(if(behavior_type='buy', behavior_type, null)) 'buy'
from user_behavior
group by dates, hours
order by dates, hours;

#统计个behavior type 的用户数量
create table user_beha_type_num (
    behavior_type varchar(5),
    num int);

insert into user_beha_type_num
select behavior_type, count(distinct user_id) num from user_behavior
group by behavior_type
order by behavior_type desc;


#统计各个behavior type 的类型
create table beha_type_num (
    behavior_type varchar(5),
    num int);
insert into beha_type_num
select behavior_type, count(user_id) num from user_behavior
group by behavior_type
order by behavior_type desc;

##### 行为路径分析 ####

drop view user_behavior_view;
drop view user_behavior_standard;
drop view user_path;
drop view path_cnt;

create view user_behavior_view as
select user_id, item_id,
       count(if(behavior_type='pv', behavior_type, null)) 'pv',
       count(if(behavior_type='cart', behavior_type, null)) 'cart',
       count(if(behavior_type='fav', behavior_type, null)) 'fav',
       count(if(behavior_type='buy', behavior_type, null)) 'buy'
from user_behavior
group by user_id, item_id;

#将用户的行为进行standardized
create view user_behavior_standard as
select user_id, item_id,(case when pv>0 then 1 else 0 end) 已浏览,
       (case when fav>0 then 1 else 0 end) 已收藏,
       (case when cart>0 then 1 else 0 end) 已添加,
       (case when buy>0 then 1 else 0 end) 已购买
from user_behavior_view;

create view user_path as
select *, concat(已浏览,已收藏,已添加,已购买) 用户购买路径
from user_behavior_standard
where 已购买>0;

create view path_cnt as
select 用户购买路径, count(*) cnt from user_path
group by 用户购买路径
order by cnt desc;

create table pathcode(
    path_type char(4),
    description varchar(40)
);

insert into pathcode
values('0001','直接购买'),
      ('1001','浏览后购买'),
      ('0011','添加后购买'),
      ('1011','浏览添加后购买'),
      ('0101','收藏后购买'),
      ('1101','浏览收藏后购买'),
      ('0111','收藏添加后购买'),
      ('1111','浏览收藏添加后购买');

select * from path_cnt t1 join pathcode t2 on t1.用户购买路径 = t2.path_type
order by cnt;

create table user_path_result(
    path_type char(4),
    description varchar(40),
    num int);

insert into user_path_result
select path_type, description, cnt
from path_cnt t1 join pathcode t2 on t1.用户购买路径 = t2.path_type;

##### RFM #######

# Recency 最近一次购买 & Frequency 购买频率
create table rfmModel(
     user_id int, recency char(10) , frequency int
);

insert into rfmModel
select user_id, max(dates) recency, count(user_id) frequency from user_behavior where behavior_type='buy'
group by user_id order by 2 desc, 3 desc;


#根据recency 进行用户分层
alter table rfmModel add column rscore int;
update rfmModel set rscore = (case when recency = '2017-12-03' then 5
    when recency in ('2017-12-01','2017-12-02') then 4
    when recency in ('2017-11-29','2017-11-30') then 3
    when recency in ('2017-11-27','2017-11-28') then 2
    else 1 end
    );

#根据frequency 进行用户分层
alter table rfmModel add column fscore int;
update rfmModel set fscore = (case when frequency between 100 and 262 then 5
    when frequency between 50 and 99 then 4
    when frequency between  20 and 49 then 3
    when frequency between 5 and 19 then 2
    else 1 end);


set @favg = null;
set @ravg = null;
select avg(fscore) into @favg from rfmModel;
select avg(rscore) into @ravg from rfmModel;

alter table rfmModel add column category varchar(40);
update rfmModel
set category = case
when fscore>@favg and rscore >@ravg then '价值用户'
when fscore>@favg and rscore <@ravg then '保持用户'
when fscore<@favg and rscore >@ravg then '发展用户'
when fscore<@favg and rscore <@ravg then '挽留用户'
end;


#### 商品热度排行 ####
#种类热度分类 Top 10
create table popular10_categories(
    category_id int,
    pv int);
insert into popular10_categories
select category_id, count(if(behavior_type='pv', behavior_type, null)) '品类浏览量'from user_behavior
group by category_id order by 2 desc limit 10;

#商品热度 Top 10
create table popular10_items(
    item_id int,
    pv int);
insert into popular10_items
select item_id, count(if(behavior_type='pv',behavior_type,null)) '商品浏览量' from user_behavior
group by item_id order by 2 desc limit 10;

#品类商品热度
create table popular10_cateitems(
    category_id int,
    item_id int,
    pv int);
insert into popular10_cateitems
select category_id, item_id, 品类商品浏览量 from (
     select category_id, item_id,
            count(if(behavior_type='pv', behavior_type, null)) '品类商品浏览量',
            rank() over(partition by category_id order by '品类商品浏览量' desc) rk
     from user_behavior
     group by category_id, item_id
     order by 3 desc
) t1
where t1.rk = 1
order by t1.品类商品浏览量 desc
limit 10;

#### 商品转化率 ####
#商品转化率
create table  item_convert_rate(
    item_id int, pv int, fav int, cart int, buy int, purchase_item_rate float
);
insert into item_convert_rate
select item_id, count(if(behavior_type='pv', behavior_type,null)) pv,
       count(if(behavior_type='fav', behavior_type,null)) fav,
       count(if(behavior_type='cart',behavior_type,null)) cart,
       count(if(behavior_type='buy',behavior_type,null)) buy,
       count(if(behavior_type='buy',behavior_type,null))/count(distinct user_id) 商品转化率
from user_behavior
group by item_id
order by 商品转化率 desc;

#品类转化率

create table  category_convert_rate(
   item_id int, pv int, fav int, cart int, buy int, purchase_category_rate float
);
insert into category_convert_rate
select category_id, count(if(behavior_type='pv', behavior_type,null)) pv,
       count(if(behavior_type='fav', behavior_type,null)) fav,
       count(if(behavior_type='cart',behavior_type,null)) cart,
       count(if(behavior_type='buy',behavior_type,null)) buy,
       count(if(behavior_type='buy',behavior_type,null))/count(distinct user_id) 品种转化率
from user_behavior
group by category_id
order by 品种转化率 desc;

select * from popular10_cateitems;


drop table tendency;
create table tendency(
    dh char(13),
    pv int,
    fav int,
    cart int,
    buy int
);
insert into tendency
select concat(date,' ',hours) dv, pv, fav, cart, buy from hourstimes_behav;

select * from hourstimes_behav;

select hours, (sum(buy) / sum(pv)) ratio from hourstimes_behav group by hours;

select count(1) from user_behavior #100095129

select num/100095129 from beha_type_num where behavior_type='pv';
select num/100095129 from beha_type_num where behavior_type='cart';



#漏斗模型
create table funnelModel(
    pv float,
    pv2favcart float,
    favcart2buy float,
    pv2buy float
);
insert into funnelModel
select pv/pv pv,
       (fav+cart)/pv pv2favcart,
       buy/(fav+cart) favcart2buy,
       buy/pv pv2buy from (
      select sum(if(behavior_type='pv',num,null)) pv,
             sum(if(behavior_type='fav',num,null)) fav,
             sum(if(behavior_type='cart',num,null)) cart,
             sum(if(behavior_type='buy',num,null)) buy
      from beha_type_num
)t1;

create table tunnelModel2 (
    num float,
    type varchar(15)
);
insert into tunnelModel2
select pv*100 num , 'pv' type from funnelModel
union all
select pv2favcart*100 num, 'pv2favcart' type from funnelModel
union all
select favcart2buy*100 num, 'favcart2buy' type from funnelModel
union all
select pv2buy num , 'pv2buy' type from funnelModel;

describe user_behavior
```