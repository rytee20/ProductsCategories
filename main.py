from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


def Connection():
    spark = SparkSession.builder \
        .appName("Read from PostgreSQL") \
        .config("spark.driver.extraClassPath", "C:/Users/user/Downloads/postgresql-42.7.3.jar") \
        .getOrCreate()
    
    db = {
        "driver": "org.postgresql.Driver",
        "url": "jdbc:postgresql://localhost:5432/products_categories_db",
        "user": "postgres",
        "password": "qwerty22"
    }

    return spark, db

def Read_Data(spark, db, table_name):
    df = spark.read.jdbc(url=db["url"],
                         table=table_name,
                         properties=db)
    
    return df


def Product_Category(df_poducts, df_categories, df_product_categories):
    #ПАРЫ ПРОДУКТ-КАТЕГОРИЯ
    product_category_df = df_product_categories.join(df_poducts, "product_id") \
        .join(df_categories, "category_id") \
            .select("product_name", "category_name")
    
    #ПРОДУКТЫ БЕЗ КАТЕГОРИИ
    products_with_no_category_df = df_poducts.join(df_product_categories, "product_id", "left_outer") \
        .filter(df_product_categories["category_id"].isNull()) \
            .select("product_name")
    
    #ОБЪЕДИНЯЕМ
    result_df = product_category_df.union(products_with_no_category_df.select("product_name", lit(None).alias("category_name")))

    return result_df, product_category_df, products_with_no_category_df


if __name__ == '__main__':

    spark, db_properties = Connection()

    df_poducts = Read_Data(spark, db_properties, "products")
    df_categories = Read_Data(spark, db_properties, "categories")
    df_product_categories = Read_Data(spark, db_properties, "product_categories")


    result_df, product_category_df, products_with_no_category_df = Product_Category(df_poducts, df_categories, df_product_categories)

    print('Product-category pairs:')
    product_category_df.show()

    print('Products without category:')
    products_with_no_category_df.show()

    print('Result:')
    result_df.show()
