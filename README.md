# AIDPのサンプルコード
### demo_notebook_01.ipynb シンプルなデータ処理のサンプルコード
# PySparkのデータ処理例
# Create a sample DataFrame
data = [("Alice", 30), ("Bob", 35), ("Charlie", 25)]
columns = ["name", "age"]
df = spark.createDataFrame(data, columns)
# DataFrame全体を表示
df.show()
# スキーマを確認
df.printSchema()
# 最初の数行を取得
df.head(2)
# 列の操作

from pyspark.sql.functions import col

# 列を選択
df.select("name").show()
# 新しい列を追加
df = df.withColumn("age_plus_10", col("age") + 10)
df.show()
# 列の名前を変更
df = df.withColumnRenamed("age", "years")
df.show()
# フィルタリング

# age > 30 の行だけ抽出
df.filter(col("years") > 30).show()

# 複数条件
df.filter((col("years") > 25) & (col("name") != "Alice")).show()
# 集計 / グループ化

from pyspark.sql.functions import avg, max, min

# 年齢の平均
df.select(avg("years")).show()

# 最大・最小
df.select(max("years"), min("years")).show()
# ソート処理

# 年齢順に昇順
df.orderBy("years").show()

# 年齢順に降順
df.orderBy(col("years").desc()).show()
# 集計処理

# 行数を数える
df.count()

# 重複を削除
df.dropDuplicates().show()

# SQLクエリ

# 一時テーブルに登録
df.createOrReplaceTempView("people")

# SQLでクエリ
spark.sql("SELECT name, years FROM people WHERE years > 30").show()

# SparkSQLのデータ処理例
%sql
CREATE OR REPLACE TEMP VIEW people AS
SELECT * FROM VALUES
  ('Alice', 30),
  ('Bob', 35),
  ('Charlie', 25)
AS people(name, age);
%sql
select * from people
%sql
-- -- age > 30 の行だけ抽出
SELECT * FROM people WHERE age > 30;

%sql
-- 複数条件 (age > 25 AND name != 'Alice')
SELECT *
FROM people
WHERE age > 25
  AND name <> 'Alice';
### demo_notebook_02.ipynb 機械学習のサンプルコード
### demo_notebook_03.ipynb ETLのサンプルコード
### demo_notebook_04.ipynb 生成AI(OCI Generative AI)を使ったサンプルコード
