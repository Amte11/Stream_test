-- 设置本地模式和动态分区
set hive.exec.mode.local.auto=True;
set hive.exec.dynamic.partition.mode=nonstrict;

-- 创建数据库和表（保持不变）
create database if not exists tms01;
use tms01;

drop table if exists dwd_trade_order_detail;
create external table dwd_trade_order_detail(
  `id` bigint comment '运单明细ID',
  `order_id` string COMMENT '运单ID',
  `cargo_type` string COMMENT '货物类型ID',
  `cargo_type_name` string COMMENT '货物类型名称',
  `volume_length` bigint COMMENT '长cm',
  `volume_width` bigint COMMENT '宽cm',
  `volume_height` bigint COMMENT '高cm',
  `weight` decimal(16,2) COMMENT '重量 kg',
  `order_time` string COMMENT '下单时间',
  `order_no` string COMMENT '运单号',
  `status` string COMMENT '运单状态',
  `status_name` string COMMENT '运单状态名称',
  `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
  `collect_type_name` string COMMENT '取件类型名称',
  `user_id` bigint COMMENT '用户ID',
  `receiver_complex_id` bigint COMMENT '收件人小区id',
  `receiver_province_id` string COMMENT '收件人省份id',
  `receiver_city_id` string COMMENT '收件人城市id',
  `receiver_district_id` string COMMENT '收件人区县id',
  `receiver_name` string COMMENT '收件人姓名',
  `sender_complex_id` bigint COMMENT '发件人小区id',
  `sender_province_id` string COMMENT '发件人省份id',
  `sender_city_id` string COMMENT '发件人城市id',
  `sender_district_id` string COMMENT '发件人区县id',
  `sender_name` string COMMENT '发件人姓名',
  `cargo_num` bigint COMMENT '货物个数',
  `amount` decimal(16,2) COMMENT '金额',
  `estimate_arrive_time` string COMMENT '预计到达时间',
  `distance` decimal(16,2) COMMENT '距离，单位：公里'
) comment '交易域订单明细事务事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_trade_order_detail'
    tblproperties('orc.compress' = 'snappy');

-- 修正后的INSERT语句
insert overwrite table dwd_trade_order_detail
    partition (dt)
select
    cargo.id,
    order_id,
    cargo_type,
    dic_for_cargo_type.name               as cargo_type_name,
    volume_length,
    volume_width,
    volume_height,
    weight,
    order_time,
    order_no,
    status,
    dic_for_status.name                   as status_name,
    collect_type,
    dic_for_collect_type.name             as collect_type_name,
    user_id,
    receiver_complex_id,
    receiver_province_id,
    receiver_city_id,
    receiver_district_id,
    receiver_name,
    sender_complex_id,
    sender_province_id,
    sender_city_id,
    sender_district_id,
    sender_name,
    cargo_num,
    amount,
    estimate_arrive_time,
    distance,
    date_format(to_date(order_time), 'yyyy-MM-dd') as dt  -- 确保日期格式正确
from (
    select
        id,
        order_id,
        cargo_type,
        volume_length,
        volume_width,
        volume_height,
        weight,
        concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) as order_time
    from ods_order_cargo
    where dt = '2025-07-12'
        and is_deleted = '0'
) cargo
join (
    select
        id,
        order_no,
        status,
        collect_type,
        user_id,
        receiver_complex_id,
        receiver_province_id,
        receiver_city_id,
        receiver_district_id,
        concat(substr(receiver_name, 1, 1), '*') as receiver_name,
        sender_complex_id,
        sender_province_id,
        sender_city_id,
        sender_district_id,
        concat(substr(sender_name, 1, 1), '*') as sender_name,
        cargo_num,
        amount,
        date_format(from_utc_timestamp(
            cast(estimate_arrive_time as bigint), 'UTC'),
            'yyyy-MM-dd HH:mm:ss') as estimate_arrive_time,
        distance
    from ods_order_info
    where dt = '2025-07-12'
        and is_deleted = '0'
) info
on cargo.order_id = info.id
left join (
    select
        id,
        name
    from ods_base_dic
    where dt = '2025-07-12'
        and is_deleted = '0'
) dic_for_cargo_type
on cargo.cargo_type = dic_for_cargo_type.id  -- 直接使用ID，避免类型转换
left join (
    select
        id,
        name
    from ods_base_dic
    where dt = '2025-07-12'
        and is_deleted = '0'
) dic_for_status
on info.status = dic_for_status.id  -- 直接使用ID，避免类型转换
left join (
    select
        id,
        name
    from ods_base_dic
    where dt = '2025-07-12'
        and is_deleted = '0'
) dic_for_collect_type
on info.collect_type = dic_for_collect_type.id;  -- 修正为dic_for_collect_type

-- 查询验证
select * from dwd_trade_order_detail limit 10;