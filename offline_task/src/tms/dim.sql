set hive.exec.mode.local.auto=True;
create database if not exists tms01;
use tms01;

-- 小区维度表
drop table if exists dim_complex_full;
create external table dim_complex_full(
  `id` bigint comment '小区ID',
  `complex_name` string comment '小区名称',
  `courier_emp_ids` array<string> comment '负责快递员IDS',
  `province_id` bigint comment '省份ID',
  `province_name` string comment '省份名称',
  `city_id` bigint comment '城市ID',
  `city_name` string comment '城市名称',
  `district_id` bigint comment '区（县）ID',
  `district_name` string comment '区（县）名称'
) comment '小区维度表'
  partitioned by (`dt` string comment '统计日期')
  stored as orc
  location '/warehouse/tms/dim/dim_complex_full'
  tblproperties('orc.compress'='snappy');

-- 插入数据（修正后的查询）
insert overwrite table dim_complex_full
    partition (dt = '2025-07-12')
select
    complex_info.id as id,
    complex_info.complex_name,
    complex_courier.courier_emp_ids,
    complex_info.province_id,
    dic_prov.name as province_name,
    complex_info.city_id,
    dic_city.name as city_name,
    complex_info.district_id,
    complex_info.district_name
from (
         select
             id,
             complex_name,
             province_id,
             city_id,
             district_id,
             district_name
         from ods_base_complex
         where dt = '2025-07-12'
           and is_deleted = '0'
     ) complex_info
-- 关联省份信息
         join (
    select
        id,
        name
    from ods_base_region_info
    where dt = '2025-07-12'
      and is_deleted = '0'
) dic_prov
              on complex_info.province_id = dic_prov.id
-- 关联城市信息
         join (
    select
        id,
        name
    from ods_base_region_info
    where dt = '2025-07-12'
      and is_deleted = '0'
) dic_city
              on complex_info.city_id = dic_city.id
-- 左关联快递员信息
         left join (
    select
        complex_id,
        collect_set(cast(courier_emp_id as string)) as courier_emp_ids
    from ods_express_courier_complex
    where dt = '2025-07-12'
      and is_deleted = '0'
    group by complex_id
) complex_courier
                   on complex_info.id = complex_courier.complex_id;

-- 验证数据
select * from dim_complex_full limit 10;

-- 快递员维度表

drop table if exists dim_express_courier_full;
create external table dim_express_courier_full(
  `id` bigint COMMENT '快递员ID',
  `emp_id` bigint COMMENT '员工ID',
  `org_id` bigint COMMENT '所属机构ID',
  `org_name` string COMMENT '机构名称',
  `working_phone` string COMMENT '工作电话',
  `express_type` string COMMENT '快递员类型（收货；发货）',
  `express_type_name` string COMMENT '快递员类型名称'
) comment '快递员维度表'
  partitioned by (`dt` string comment '统计日期')
  stored as orc
  location '/warehouse/tms/dim/dim_express_courier_full'
  tblproperties('orc.compress'='snappy');

insert overwrite table dim_express_courier_full
    partition (dt = '2025-07-12')
select express_cor_info.id,
       emp_id,
       org_id,
       org_name,
       working_phone,
       express_type,
       dic_info.name express_type_name
from (select id,
             emp_id,
             org_id,
             working_phone,
             express_type
      from ods_express_courier
      where dt = '2025-07-12'
        and is_deleted = '0') express_cor_info
         join (
    select id,
           org_name
    from ods_base_organ
    where dt = '2025-07-12'
      and is_deleted = '0'
) organ_info
              on express_cor_info.org_id = organ_info.id
         join (
    select id,
           name
    from ods_base_dic
    where dt = '2025-07-12'
      and is_deleted = '0'
) dic_info
              on express_type = dic_info.id;

select * from dim_express_courier_full;

--机构维度表
drop table if exists dim_organ_full;
create external table dim_organ_full(
  `id` bigint COMMENT '机构ID',
  `org_name` string COMMENT '机构名称',
  `org_level` bigint COMMENT '机构等级（1为转运中心，2为转运站）',
  `region_id` bigint COMMENT '地区ID，1级机构为city ,2级机构为district',
  `region_name` string COMMENT '地区名称',
  `region_code` string COMMENT '地区编码（行政级别）',
  `org_parent_id` bigint COMMENT '父级机构ID',
  `org_parent_name` string COMMENT '父级机构名称'
) comment '机构维度表'
  partitioned by (`dt` string comment '统计日期')
  stored as orc
  location '/warehouse/tms/dim/dim_organ_full'
  tblproperties('orc.compress'='snappy');


insert overwrite table dim_organ_full
    partition (dt = '2025-07-12')
select organ_info.id,
       organ_info.org_name,
       org_level,
       region_id,
       region_info.name        region_name,
       region_info.dict_code   region_code,
       org_parent_id,
       organ_info.org_name org_parent_name
from (select id,
             org_name,
             org_level,
             region_id,
             org_parent_id
      from ods_base_organ
      where dt = '2025-07-12'
        and is_deleted = '0') organ_info
         left join (
    select id,
           name,
           dict_code
    from ods_base_region_info
    where dt = '2025-07-12'
      and is_deleted = '0'
) region_info
                   on organ_info.region_id = region_info.id
         left join (
    select id,
           org_name
    from ods_base_organ
    where dt = '2025-07-12'
      and is_deleted = '0'
) org_for_parent
                   on organ_info.org_parent_id = org_for_parent.id;

select * from dim_organ_full;

--地区维度表

drop table if exists dim_region_full;
create external table dim_region_full(
  `id` bigint COMMENT '地区ID',
  `parent_id` bigint COMMENT '上级地区ID',
  `name` string COMMENT '地区名称',
  `dict_code` string COMMENT '编码（行政级别）',
  `short_name` string COMMENT '简称'
) comment '地区维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_region_full'
    tblproperties('orc.compress'='snappy');

insert overwrite table dim_region_full
    partition (dt = '2025-07-12')
select id,
       parent_id,
       name,
       dict_code,
       short_name
from ods_base_region_info
where dt = '2025-07-12'
  and is_deleted = '0';

select * from dim_region_full;

--班次维度表
drop table if exists dim_shift_full;
create external table dim_shift_full(
  `id` bigint COMMENT '班次ID',
  `line_id` bigint COMMENT '线路ID',
  `line_name` string COMMENT '线路名称',
  `line_no` string COMMENT '线路编号',
  `line_level` string COMMENT '线路级别',
  `org_id` bigint COMMENT '所属机构',
  `transport_line_type_id` string COMMENT '线路类型ID',
  `transport_line_type_name` string COMMENT '线路类型名称',
  `start_org_id` bigint COMMENT '起始机构ID',
  `start_org_name` string COMMENT '起始机构名称',
  `end_org_id` bigint COMMENT '目标机构ID',
  `end_org_name` string COMMENT '目标机构名称',
  `pair_line_id` bigint COMMENT '配对线路ID',
  `distance` decimal(10,2) COMMENT '直线距离',
  `cost` decimal(10,2) COMMENT '公路里程',
  `estimated_time` bigint COMMENT '预计时间（分钟）',
  `start_time` string COMMENT '班次开始时间',
  `driver1_emp_id` bigint COMMENT '第一司机',
  `driver2_emp_id` bigint COMMENT '第二司机',
  `truck_id` bigint COMMENT '卡车ID',
  `pair_shift_id` bigint COMMENT '配对班次(同一辆车一去一回的另一班次)'
) comment '班次维度表'
    partitioned by (`dt` string comment '统计周期')
    stored as orc
    location '/warehouse/tms/dim/dim_shift_full'
    tblproperties('orc.compress'='snappy');

insert overwrite table dim_shift_full
    partition (dt = '2025-07-12')
select shift_info.id,
       line_id,
       line_info.name line_name,
       line_no,
       line_level,
       org_id,
       transport_line_type_id,
       dic_info.name  transport_line_type_name,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       pair_line_id,
       distance,
       cost,
       estimated_time,
       start_time,
       driver1_emp_id,
       driver2_emp_id,
       truck_id,
       pair_shift_id
from (select id,
             line_id,
             start_time,
             driver1_emp_id,
             driver2_emp_id,
             truck_id,
             pair_shift_id
      from ods_line_base_shift
      where dt = '2025-07-12'
        and is_deleted = '0') shift_info
         join
     (select id,
             name,
             line_no,
             line_level,
             org_id,
             transport_line_type_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             pair_line_id,
             distance,
             cost,
             estimated_time
      from ods_line_base_info
      where dt = '2025-07-12'
        and is_deleted = '0') line_info
     on shift_info.line_id = line_info.id
         join (
    select id,
           name
    from ods_base_dic
    where dt = '2025-07-12'
      and is_deleted = '0'
) dic_info on line_info.transport_line_type_id = dic_info.id;

select * from dim_shift_full;

--司机维度表
drop table if exists dim_truck_driver_full;
create external table dim_truck_driver_full(
  `id` bigint COMMENT '司机信息ID',
  `emp_id` bigint COMMENT '员工ID',
  `org_id` bigint COMMENT '所属机构ID',
  `org_name` string COMMENT '所属机构名称',
  `team_id` bigint COMMENT '所属车队ID',
  `tream_name` string COMMENT '所属车队名称',
  `license_type` string COMMENT '准驾车型',
  `init_license_date` string COMMENT '初次领证日期',
  `expire_date` string COMMENT '有效截止日期',
  `license_no` string COMMENT '驾驶证号',
  `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '司机维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_truck_driver_full'
    tblproperties('orc.compress'='snappy');

insert overwrite table dim_truck_driver_full
partition (dt = '2025-07-12')
select
    driver_info.id,
    emp_id,
    driver_info.org_id,  -- 保留原始 org_id，方便排查
    organ_info.org_name,
    driver_info.team_id, -- 保留原始 team_id，方便排查
    team_info.name team_name,
    license_type,
    -- 用 COALESCE 处理空值，赋予默认时间（如 '1970-01-01 00:00:00'）
    coalesce(init_license_date, '1970-01-01 00:00:00') init_license_date,
    coalesce(expire_date, '2099-12-31 23:59:59') expire_date,
    license_no,
    is_enabled
from (select id,
             emp_id,
             org_id,
             team_id,
             license_type,
             init_license_date,
             expire_date,
             license_no,
             is_enabled
      from ods_truck_driver
      where dt = '2025-07-12'
        and is_deleted = '0') driver_info
-- 左连接，确保司机主表数据不丢失
         left join (
    select id,
           org_name
    from ods_base_organ
    where dt = '2025-07-12'
      and is_deleted = '0'
) organ_info
                   on driver_info.org_id = organ_info.id
-- 左连接，确保司机主表数据不丢失
         left join (
    select id,
           name
    from ods_truck_team
    where dt = '2025-07-12'
      and is_deleted = '0'
) team_info
                   on driver_info.team_id = team_info.id;

select * from dim_truck_driver_full;


--卡车维度表

drop table if exists dim_truck_full;
create external table dim_truck_full(
  `id` bigint COMMENT '卡车ID',
  `team_id` bigint COMMENT '所属车队ID',
  `team_name` string COMMENT '所属车队名称',
  `team_no` string COMMENT '车队编号',
  `org_id` bigint COMMENT '所属机构',
  `org_name` string COMMENT '所属机构名称',
  `manager_emp_id` bigint COMMENT '负责人',
  `truck_no` string COMMENT '车牌号码',
  `truck_model_id` string COMMENT '型号',
  `truck_model_name` string COMMENT '型号名称',
  `truck_model_type` string COMMENT '型号类型',
  `truck_model_type_name` string COMMENT '型号类型名称',
  `truck_model_no` string COMMENT '型号编码',
  `truck_brand` string COMMENT '品牌',
  `truck_brand_name` string COMMENT '品牌名称',
  `truck_weight` decimal(16,2) COMMENT '整车重量（吨）',
  `load_weight` decimal(16,2) COMMENT '额定载重（吨）',
  `total_weight` decimal(16,2) COMMENT '总质量（吨）',
  `eev` string COMMENT '排放标准',
  `boxcar_len` decimal(16,2) COMMENT '货箱长（m）',
  `boxcar_wd` decimal(16,2) COMMENT '货箱宽（m）',
  `boxcar_hg` decimal(16,2) COMMENT '货箱高（m）',
  `max_speed` bigint COMMENT '最高时速（千米/时）',
  `oil_vol` bigint COMMENT '油箱容积（升）',
  `device_gps_id` string COMMENT 'GPS设备ID',
  `engine_no` string COMMENT '发动机编码',
  `license_registration_date` string COMMENT '注册时间',
  `license_last_check_date` string COMMENT '最后年检日期',
  `license_expire_date` string COMMENT '失效日期',
  `is_enabled` tinyint COMMENT '状态 0：禁用 1：正常'
) comment '卡车维度表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_truck_full'
    tblproperties('orc.compress'='snappy');

insert overwrite table dim_truck_full
partition (dt = '2025-07-12')
select truck_info.id,
       team_id,
       team_info.name     team_name,
       team_no,
       org_id,
       org_name,
       manager_emp_id,
       truck_no,
       truck_model_id,
       model_name         truck_model_name,
       model_type         truck_model_type,
       dic_for_type.name  truck_model_type_name,
       model_no           truck_model_no,
       brand              truck_brand,
       dic_for_brand.name truck_brand_name,
       truck_weight,
       load_weight,
       total_weight,
       eev,
       boxcar_len,
       boxcar_wd,
       boxcar_hg,
       max_speed,
       oil_vol,
       device_gps_id,
       engine_no,
       license_registration_date,
       license_last_check_date,
       license_expire_date,
       is_enabled
from (select id,
             team_id,

             md5(truck_no) truck_no,
             truck_model_id,

             device_gps_id,
             engine_no,
             license_registration_date,
             license_last_check_date,
             license_expire_date,
             is_enabled
      from ods_truck_info
      where dt = '2025-07-12'
        and is_deleted = '0') truck_info
         join
     (select id,
             name,
             team_no,
             org_id,

             manager_emp_id
      from ods_truck_team
      where dt = '2025-07-12'
        and is_deleted = '0') team_info
     on truck_info.team_id = team_info.id
         join
     (select id,
             model_name,
             model_type,

             model_no,
             brand,

             truck_weight,
             load_weight,
             total_weight,
             eev,
             boxcar_len,
             boxcar_wd,
             boxcar_hg,
             max_speed,
             oil_vol
      from ods_truck_model
      where dt = '2025-07-12'
        and is_deleted = '0') model_info
     on truck_info.truck_model_id = model_info.id
         join
     (select id,
             org_name
      from ods_base_organ
      where dt = '2025-07-12'
        and is_deleted = '0'
     ) organ_info
     on org_id = organ_info.id
         join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-12'
        and is_deleted = '0') dic_for_type
     on model_info.model_type = dic_for_type.id
         join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-12'
        and is_deleted = '0') dic_for_brand
     on model_info.brand = dic_for_brand.id;

select * from dim_truck_full;

-- 用户地址拉链表
drop table if exists dim_user_address_zip;
create external table dim_user_address_zip(
  `id` bigint COMMENT '地址ID',
  `user_id` bigint COMMENT '用户ID',
  `phone` string COMMENT '电话号',
  `province_id` bigint COMMENT '所属省份ID',
  `city_id` bigint COMMENT '所属城市ID',
  `district_id` bigint COMMENT '所属区县ID',
  `complex_id` bigint COMMENT '所属小区ID',
  `address` string COMMENT '详细地址',
  `is_default` tinyint COMMENT '是否默认',
  `start_date` string COMMENT '起始日期',
  `end_date` string COMMENT '结束日期'
) comment '用户地址拉链表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_address_zip'
    tblproperties('orc.compress'='snappy');

insert overwrite table dim_user_address_zip
    partition (dt = '2025-07-12')
select after.id,
       after.user_id,
       after.phone,
       after.province_id,
       after.city_id,
       after.district_id,
       after.complex_id,
       after.address,
       after.is_default,
       concat(substr(after.create_time, 1, 10), ' ',
              substr(after.create_time, 12, 8)) start_date,
       '9999-12-31'                             end_date
from ods_user_address after
where dt = '2025-07-12'
  and after.is_deleted = '0';

select * from dim_user_address_zip;

-- 用户维度表
drop table if exists dim_user_zip;
create external table dim_user_zip(
  `id` bigint COMMENT '用户地址信息ID',
  `login_name` string COMMENT '用户名称',
  `nick_name` string COMMENT '用户昵称',
  `passwd` string COMMENT '用户密码',
  `real_name` string COMMENT '用户姓名',
  `phone_num` string COMMENT '手机号',
  `email` string COMMENT '邮箱',
  `user_level` string COMMENT '用户级别',
  `birthday` string COMMENT '用户生日',
  `gender` string COMMENT '性别 M男,F女',
  `start_date` string COMMENT '起始日期',
  `end_date` string COMMENT '结束日期'
) comment '用户拉链表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dim/dim_user_zip'
    tblproperties('orc.compress'='snappy');

-- 3）首日装载
insert overwrite table dim_user_zip
    partition (dt = '2025-07-12')
select after.id,
       after.login_name,
       after.nick_name,
       after.passwd,
       after.real_name,
       after.phone_num,
       after.email,
       after.user_level,
       date_add('1970-01-01', cast(after.birthday as int)) birthday,
       after.gender,
       date_format(from_utc_timestamp(
                           cast(after.create_time as bigint), 'UTC'),
                   'yyyy-MM-dd') start_date,
       '9999-12-31' end_date
from ods_user_info after
where dt = '2025-07-12'
  and after.is_deleted = '0';

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_user_zip
    partition (dt)
select id,
       login_name,
       nick_name,
       passwd,
       real_name,
       phone_num,
       email,
       user_level,
       birthday,
       gender,
       start_date,
       if(rk = 1, end_date, date_add('2025-07-13', -1)) end_date,
       if(rk = 1, end_date, date_add('2025-07-13', -1)) dt
from (
         select
             id,
             login_name,
             nick_name,
             passwd,
             real_name,
             phone_num,
             email,
             user_level,
             birthday,
             gender,
             start_date,
             end_date,
             row_number() over (partition by id order by start_date desc) rk
         from (
                  -- 子查询 1：从拉链表取历史数据（这里原条件 dt='2025-07-13' 可能有问题，首日装载 dt 是 2025-07-12，按需调整，假设是取已有分区数据）
                  select
                      id,
                      login_name,
                      nick_name,
                      passwd,
                      real_name,
                      phone_num,
                      email,
                      user_level,
                      birthday,
                      gender,
                      start_date,
                      end_date
                  from dim_user_zip
                  where dt = '2025-07-12'  -- 结合首日装载逻辑，调整为已存在的分区，比如首日的 2025-07-12
                  union all  -- 用 union all 更高效，且明确是合并结果，union 会去重，这里不需要
                  -- 子查询 2：从 ods 取增量数据
                  select
                      inc_inner.id,
                      inc_inner.login_name,
                      inc_inner.nick_name,
                      inc_inner.passwd,  -- 原错误处修正，确保列对应
                      inc_inner.real_name,
                      inc_inner.phone_num,
                      inc_inner.email,
                      inc_inner.user_level,
                      cast(date_add('1970-01-01', cast(inc_inner.birthday as int)) as string) birthday,
                      inc_inner.gender,
                      '2025-07-13' start_date,
                      '9999-12-31' end_date
                  from (
                           select
                               after.id,
                               after.login_name,
                               after.nick_name,
                               after.passwd,
                               after.real_name,
                               after.phone_num,
                               after.email,
                               after.user_level,
                               after.birthday,
                               after.gender,
                               row_number() over (partition by after.id order by dt desc) rn
                           from ods_user_info after
                           where dt = '2025-07-13'
                             and after.is_deleted = '0'
                       ) inc_inner
                  where inc_inner.rn = 1
              ) full_info
     ) final_info;

select * from dim_user_zip;


