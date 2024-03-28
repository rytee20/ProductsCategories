# ProductsCategories

![image](https://github.com/rytee20/ProductsCategories/assets/94058290/df4200cf-f8cb-45f4-a73b-693594298efa)


## База Данных

Для примера была создана база данных PostgreSQL:
```
CREATE DATABASE products_categories_db;
```

**Таблица Продукты:**
```
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) UNIQUE NOT NULL
);

INSERT INTO products (product_name) VALUES ('Product A'), ('Product B'), ('Product C'), ('Product D');
```
| product_id | product_name |
|:---------:|:---------:|
|         1|   Product A|
|         2|   Product B|
|         3|   Product C|
|         4|   Product D|

**Таблица Категорий:**
```
CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) UNIQUE NOT NULL
);

INSERT INTO categories (category_name) VALUES ('Category 1'), ('Category 2'), ('Category 3');
```

| category_id | category_name |
|:---------:|:---------:|
|         1|   Category 1|
|         2|   Category 2|
|         3|   Category 3|

**Таблица Связей:**
```
CREATE TABLE product_categories (
    product_id INT REFERENCES products(product_id),
    category_id INT REFERENCES categories(category_id),
    PRIMARY KEY (product_id, category_id)
);

INSERT INTO product_categories (product_id, category_id) VALUES (1, 1), (1, 2), (2, 2), (3, 1), (3, 3);
```

| roduct_id | category_id |
|:---------:|:---------:|
|         1|          1|
|         1|          2|
|         2|          2|
|         3|          1|
|         3|          3|

## Код

В функции `Connection()` создается SparkSession и подключение к базе данных PostgreSQL.

Функция `Read_Data(spark, db, table_name)` предназначена для чтения таблицы в DataFrame.

Функция `Product_Category(df_poducts, df_categories, df_product_categories)` возвращает три DataFrame:
- `result_df`: все пары «Имя продукта – Имя категории», у пробукторв без категории в поле category_name указано NULL
  
| roduct_name | category_name |
|:---------:|:---------:|
|   Product C|   Category 1|
|   Product A|   Category 1|
|   Product C|   Category 3|
|   Product B|   Category 2|
|   Product A|   Category 2|
|   Product D|         NULL|

- `product_category_df`: все пары «Имя продукта – Имя категории»

| roduct_name | category_name |
|:---------:|:---------:|
|   Product C|   Category 1|
|   Product A|   Category 1|
|   Product C|   Category 3|
|   Product B|   Category 2|
|   Product A|   Category 2|


- `products_with_no_category_df`: имена всех продуктов, у которых нет категорий

|product_name|
|:---------:|
|   Product D|
