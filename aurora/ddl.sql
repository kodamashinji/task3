-- 店舗情報
create table shop (
    shop_index int unsigned not null auto_increment             -- 店舗内部ID
    , name varchar(256) not null                                -- 店舗名
    , status tinyint unsigned not null default 0                -- 閉店かどうかなど
    , geohash varbinary(20) not null                            -- 座標(geohash形式)
    , location geometry not null srid 4326                      -- 場所(店舗の位置[WGS84])
    , created_at timestamp default current_timestamp not null   -- 作成日(自動セット)
    , primary key (shop_index)
    , index (geohash)
    , spatial key (location)
) engine=innodb default charset=utf8mb4;

-- クーポン情報
create table coupon (
    shop_index int unsigned not null                            -- 店舗内部ID
    , seq int unsigned not null                                 -- クーポンの通し番号
    , status tinyint unsigned not null default 0                -- 使用可能かどうかなど
    , valid_from timestamp not null                             -- 有効開始日時
    , valid_to timestamp not null                               -- 有効終了日時
    , image_key varbinary(256)                                  -- クーポン画像 (S3のキー名)
    , description text not null                                 -- 詳細
    , created_at timestamp default current_timestamp not null   -- 作成日(自動セット)
    , primary key(shop_index, seq)
    , foreign key(shop_index) references shop(shop_index) on delete cascade
) engine=innodb default charset=utf8mb4;

-- ユーザ情報
create table user (
    user_index int unsigned not null auto_increment             -- ユーザ内部ID
    , user_id char(36) not null                                 -- ユーザID (UUID)
    , name varchar(256) not null                                -- ユーザ名
    , status tinyint unsigned not null default 0                -- 有効かどうかなど
    , created_at timestamp default current_timestamp not null   -- 作成日(自動セット)
    , primary key(user_index)
    , unique index(user_id)
) engine=innodb default charset=utf8mb4;

-- ユーザ位置情報 (レコードをローテーションする)
create table user_location (
    user_index int unsigned not null                            -- ユーザ内部ID
    , slot int unsigned not null                                -- 位置情報(n個のスロットをローテーションして使用する)
    , seq int unsigned not null                                 -- 位置情報の通し番号
    , geohash varbinary(20) not null                            -- 座標(geohash形式)
    , location geometry not null srid 4326                      -- 座標(position[WGS84])
    , created_at timestamp default current_timestamp not null   -- 作成日(自動セット)
    , primary key(user_index, slot)
    , foreign key(user_index) references user(user_index) on delete cascade
    , index (geohash)
    , spatial key (location)
) engine=innodb;
