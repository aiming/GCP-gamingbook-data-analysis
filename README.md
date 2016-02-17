# GCP-gamingbook-data-analysis
インプレスR&amp;Dから出版された「ゲーム開発が変わる！Google Cloud Platform 実践インフラ構築」のSQLなどをまとめたリポジトリです

# Aimingでのデータ分析環境の紹介で紹介したSQLまとめ

上記書籍では、BigQueryのSQLについて書いています。書籍から直接手で打つのも煩雑なので利便性を上げるためこちらのページでコピペしやすいように、SQLをまとめます。

## 前提となるテーブル構造



### loginテーブルのイメージ

`production`というデータセットの`login`というテーブルがあるとします。そのテーブルは日ごとに分割されていて、`login20150101`から例えば今日(2016-02-17)、`login20160217`まであるとします。ユーザーがログインするたびに、レコードは増え、`user_id`フィールドには、userのid,`time`にはログインした時刻が入ります。

|user_id|time|
|:--|:---|
|3|2015-01-02-12:15:30 UTC|
|8|2015-01-02 12:16:45 UTC|
|3|2015-01-02 12:18:44 UTC|

の様なイメージです。

### 課金(get_cp)テーブルのイメージ

`production`というデータセットの`get_cp`というテーブルがあるとします。そのテーブルは日ごとに分割されていて、`get_cp20150101`から例えば今日(2016-02-17)、`get_cp20160217`まであるとします。ユーザーが課金するたびに、レコードは増え、`user_id`フィールドには、userのid,`pay_cp`には課金額、`time`には課金した時刻が入るとします。

|user_id|time|pay_cp|
|:--|:---|:---|
|3|2015-01-02-12:15:30 UTC|100|
|8|2015-01-02 12:16:45 UTC|200|
|3|2015-01-02 12:18:44 UTC|300|


## DAUを求めるSQL

BigQueryを使ってDAU(DailyActiveUser)を求めるSQLは次のようになります。

```SQL
SELECT
-- SELECTは普通のSQLと変わらない
EXACT_COUNT_DISTINCT(user_id) as dau,
-- 重複する値を除いた、distinct文は EXACT_COUNT_DISTINCT文を使う
STRFTIME_UTC_USEC(date_add(time,9,'HOUR'),"%Y-%m-%d") as date
-- STRFTIME_UTC_USECで時刻を日付を表す文字列に変換する。BQはUTCで時刻を保持するので、9時間足してJSTに変換する
FROM TABLE_DATE_RANGE(production.login,timestamp('2015-01-01'),current_timestamp())
-- loginテーブルは日ごとに分割されているのでそのすべてのテーブルを横断して集計する(UNION ALLに相当)には TABLE_DATE_RANGEを使う
GROUP BY date ORDER BY date;
```

## FQ5を求めるSQL

```SQL
SELECT exact_count_distinct(user_id) as value,
STRFTIME_UTC_USEC(date,'%Y-%m-%d') as date
FROM (
SELECT
user_id,date,
count(*) OVER (PARTITION BY user_id ORDER BY date RANGE BETWEEN 4 *24 *60 *60 *1000000 PRECEDING AND CURRENT ROW) as cnt
FROM
(
SELECT
user_id,UTC_USEC_TO_DAY(date_add(time,9,'HOUR')) as date
FROM table_date_range(production.login,timestamp('2015-01-01'),date_add(current_timestamp(),9,'HOUR'))
GROUP BY user_id,date )
)
WHERE cnt=5
GROUP BY date;
```

## MRPPUを求めるSQL

```SQL
SELECT
integer(NTH(50,QUANTILES(pcp))) AS value,
date
FROM
(select
STRFTIME_UTC_USEC(time,'%Y-%m-%d') as date,
sum(pay_cp) as pcp,
user_id
FROM production.get_cp
WHERE pay_cp >0
GROUP BY user_id,date
)
GROUP BY date ORDER BY date;
```
