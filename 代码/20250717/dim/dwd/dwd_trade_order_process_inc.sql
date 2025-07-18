set hive.exec.mode.local.auto=True;
create database if not exists tms01;
use tms01;

-- 交易域运单累积快照事实表
drop table if exists dwd_trade_order_process_inc;
create external table dwd_trade_order_process_inc(
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
  `payment_type` string COMMENT '支付方式',
  `payment_type_name` string COMMENT '支付方式名称',
  `cargo_num` bigint COMMENT '货物个数',
  `amount` decimal(16,2) COMMENT '金额',
  `estimate_arrive_time` string COMMENT '预计到达时间',
  `distance` decimal(16,2) COMMENT '距离，单位：公里',
  `start_date` string COMMENT '开始日期',
  `end_date` string COMMENT '结束日期'
) comment '交易域运单累积快照事实表'
    partitioned by (`dt` string comment '统计日期')
    stored as orc
    location '/warehouse/tms/dwd/dwd_order_process'
    tblproperties('orc.compress' = 'snappy');

-- 2）首日装载（2025-07-12）
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_order_process_inc
    partition (dt)
select
  id,
  order_id,
  cargo_type,
  name       ,
  volume_length,
  volume_width,
  volume_height,
  weight,
  order_time,
  order_no,
  status,
  dic_for_status.name                   status_name,
  collect_type,
  dic_for_collect_type.name             collect_type_name,
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
  payment_type,
  dic_for_payment_type.name             payment_type_name,
  cargo_num,
  amount,
  estimate_arrive_time,
  distance,
  order_time                       start_date,  -- 明确使用cargo子查询的order_time
  end_date                         end_date,
  end_date                         dt  -- 分区字段统一为 end_date
from (
  select
    id,
    order_id,
    cargo_type,
    volume_length,
    volume_width,
    volume_height,
    weight,
    -- 规范时间格式：拼接为完整时间字符串
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
    receiver_name,
    sender_complex_id,
    sender_province_id,
    sender_city_id,
    sender_district_id,
    sender_name,
    payment_type,
    cargo_num,
    amount,
    -- 规范时间转换：明确时区为 GMT+8
    from_utc_timestamp(
        cast(estimate_arrive_time as bigint),
        'GMT+8'
    ) as estimate_arrive_time,
    distance,
    -- 简化 end_date 逻辑：直接用条件判断赋值
    case
      when status in ('60080', '60999')
      then substr(update_time, 1, 10)
      else '9999-12-31'
    end as end_date
  from ods_order_info
  where dt = '2025-07-12'
    and is_deleted = '0'
) info
on order_id = id
left join (
  select id, name
  from ods_base_dic
  where dt = '2025-07-12'
    and is_deleted = '0'
) dic_for_cargo_type
on cargo_type = cast(dic_for_cargo_type.id as string)
left join (
  select id, name
  from ods_base_dic
  where dt = '2025-07-12'
    and is_deleted = '0'
) dic_for_status
on status = cast(dic_for_status.id as string)
left join (
  select id, name
  from ods_base_dic
  where dt = '2025-07-12'
    and is_deleted = '0'
) dic_for_collect_type
-- 修正关联条件：使用 dic_for_collect_type.id
on collect_type = cast(dic_for_collect_type.id as string)
left join (
  select id, name
  from ods_base_dic
  where dt = '2025-07-12'
    and is_deleted = '0'
) dic_for_payment_type
on payment_type = cast(dic_for_payment_type.id as string);

-- 3）每日装载（2025-07-13）
set hive.exec.dynamic.partition.mode=nonstrict;
with tmp as (
  -- 历史数据（未结束）
  select
    id,
    order_id,
    cargo_type,
    cargo_type_name,
    volume_length,
    volume_width,
    volume_height,
    weight,
    order_time,
    order_no,
    status,
    status_name,
    collect_type,
    collect_type_name,
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
    payment_type,
    payment_type_name,
    cargo_num,
    amount,
    estimate_arrive_time,
    distance,
    start_date,
    end_date
  from dwd_trade_order_process_inc
  where dt = '9999-12-31'

  union all

  -- 新增数据（2025-07-13）
  select
    id,
    order_id,
    cargo_type,
   name  cargo_type_name,
    volume_length,
    volume_width,
    volume_height,
    weight,
    order_time,
    order_no,
    status,
    name  status_name,
    collect_type,
    name   collect_type_name,
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
    payment_type,
    name   payment_type_name,
    cargo_num,
    amount,
    estimate_arrive_time,
    distance,
    date_format(order_time, 'yyyy-MM-dd') start_date,  -- 规范日期格式
    '9999-12-31'                          end_date
  from (
    select
      id,
      order_id,
      cargo_type,
      volume_length,
      volume_width,
      volume_height,
      weight,
      -- 规范时间转换：拼接为完整时间字符串
      concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) as order_time
    from ods_order_cargo
    where dt = '2025-07-13'
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
      receiver_name,
      sender_complex_id,
      sender_province_id,
      sender_city_id,
      sender_district_id,
      sender_name,
      payment_type,
      cargo_num,
      amount,
      -- 规范时间转换：明确时区为 GMT+8
      from_utc_timestamp(
          cast(estimate_arrive_time as bigint),
          'GMT+8'
      ) as estimate_arrive_time,
      distance
    from ods_order_info
    where dt = '2025-07-13'
      and is_deleted = '0'
  ) info
  on order_id = id
  left join (
    select id, name
    from ods_base_dic
    where dt = '2025-07-13'
      and is_deleted = '0'
  ) dic_for_cargo_type
  on cargo_type = cast(dic_for_cargo_type.id as string)
  left join (
    select id, name
    from ods_base_dic
    where dt = '2025-07-13'
      and is_deleted = '0'
  ) dic_for_status
  on status = cast(dic_for_status.id as string)
  left join (
    select id, name
    from ods_base_dic
    where dt = '2025-07-13'
      and is_deleted = '0'
  ) dic_for_collect_type
  -- 修正关联条件：使用 dic_for_collect_type.id
  on collect_type = cast(dic_for_collect_type.id as string)
  left join (
    select id, name
    from ods_base_dic
    where dt = '2025-07-13'
      and is_deleted = '0'
  ) dic_for_payment_type
  on payment_type = cast(dic_for_payment_type.id as string)
),
inc as (
  -- 状态、支付方式增量更新
  select
    inc_origin.id,
    inc_origin.status,
    inc_origin.payment_type,
    dic_for_payment_type.name payment_type_name
  from (
    select
      id,
      status,
      payment_type,
      row_number() over (partition by id order by dt desc) rn
    from ods_order_info
    where dt = '2025-07-13'
      and is_deleted = '0'
  ) inc_origin
  where rn = 1  -- 取最新状态
  left join (
    select id, name
    from ods_base_dic
    where dt = '2025-07-13'
      and is_deleted = '0'
  ) dic_for_payment_type
  on inc_origin.payment_type = cast(dic_for_payment_type.id as string)
)
insert overwrite table dwd_trade_order_process_inc
    partition(dt)
select
  tmp.id,
  tmp.order_id,
  tmp.cargo_type,
  tmp.cargo_type_name,
  tmp.volume_length,
  tmp.volume_width,
  tmp.volume_height,
  tmp.weight,
  tmp.order_time,
  tmp.order_no,
  inc.status,  -- 使用增量状态
  tmp.status_name,
  tmp.collect_type,
  tmp.collect_type_name,
  tmp.user_id,
  tmp.receiver_complex_id,
  tmp.receiver_province_id,
  tmp.receiver_city_id,
  tmp.receiver_district_id,
  tmp.receiver_name,
  tmp.sender_complex_id,
  tmp.sender_province_id,
  tmp.sender_city_id,
  tmp.sender_district_id,
  tmp.sender_name,
  inc.payment_type,  -- 使用增量支付方式
  inc.payment_type_name,  -- 使用增量支付方式名称
  tmp.cargo_num,
  tmp.amount,
  tmp.estimate_arrive_time,
  tmp.distance,
  tmp.start_date,
  -- 修正 end_date 逻辑：根据增量状态更新
  case
    when inc.status in ('60080', '60999')
    then '2025-07-13'
    else tmp.end_date
  end as end_date,
  -- 分区字段：与 end_date 保持一致
  case
    when inc.status in ('60080', '60999')
    then '2025-07-13'
    else tmp.end_date
  end as dt
from tmp
left join inc
on tmp.order_id = inc.id;

-- 验证查询（限制结果行数，避免数据量大时卡顿）
select *
from dwd_trade_order_process_inc;