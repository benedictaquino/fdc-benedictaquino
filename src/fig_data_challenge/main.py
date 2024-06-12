from datetime import datetime
import polars as pl
from sqlalchemy import create_engine, inspect, Column, Integer, String, DateTime, URL, ForeignKey
from sqlalchemy.orm import declarative_base
from sqlalchemy.schema import CreateSchema, MetaData
from sqlalchemy_utils import database_exists, create_database


Base = declarative_base(metadata=MetaData(schema='restaurant_data'))


class Restaurant(Base):
    __tablename__ = 'restaurants'

    id = Column(Integer, primary_key=True)
    name = Column(String)


class MenuItem(Base):
    __tablename__ = 'menu_items'

    id = Column(Integer, primary_key=True)
    restaurant_id = Column(Integer, ForeignKey('restaurants.id'), nullable=False)
    name = Column(String)
    category = Column(String)


class Ingredient(Base): 
    __tablename__ = 'ingredients'

    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('menu_items.id'), nullable=False)
    ingredients = Column(String)


class Allergen(Base): 
    __tablename__ = 'allergens'

    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('menu_items.id'), nullable=False)
    allergens = Column(String)


class Picture(Base): 
    __tablename__ = 'pictures'

    id = Column(Integer, primary_key=True)
    item_id = Column(Integer, ForeignKey('menu_items.id'), nullable=False)
    picture = Column(String)


if __name__ == "__main__":
    #region Reading data
    restaurant_data = pl.read_excel(
        'data/restaurant_data.xlsx',
        infer_schema_length=10000,
        sheet_id=0
    )

    menu_items_raw = restaurant_data['Restaurant Menu Items']
    categories_raw = restaurant_data['Reference categories']

    #endregion

    #region Cleaning Data

    menu_items_clean = (
        menu_items_raw
        .join(
            categories_raw,
            how='left',
            left_on=['Store', 'Product category'],
            right_on=['Restaurant name', 'Restaurant original category'],
        )
        .with_columns(
            menu_items_raw['']
            .str.to_datetime('New - %m/%d/%Y')
            .fill_null(datetime(2023, 1, 1, 0, 0, 0, 0))
            .alias('valid_from'),
        )
        .filter(
            pl.col('Product Name').is_not_null(),
            pl.col('Ingredients on Product Page').is_not_null(),
            pl.col('Store').is_not_null(),
        )
        .select(
            'Product Name',
            'Ingredients on Product Page',
            'Allergens and Warnings',
            'URL of primary product picture',
            'Store',
            'Fig Category 1',
            'valid_from',
        )
        .rename({
            'Product Name': 'name',
            'Ingredients on Product Page': 'ingredients',
            'Allergens and Warnings': 'allergens',
            'URL of primary product picture': 'picture',
            'Store': 'restaurant',
            'Fig Category 1': 'category',
        })
        .unique()
    )
    #endregion

    #region Normalization
    restaurants = (
        menu_items_clean
        .select('restaurant')
        .rename({'restaurant': 'name'})
        .unique()
        .sort('name')
    )
    # composite primary key name, restaurant
    menu_items = (
        menu_items_clean
        .select('name', 'restaurant', 'category')
        .unique()
    )
    pictures = (
        menu_items_clean
        .select('name', 'restaurant', 'picture')
        .rename({'name': 'product'})
        .unique()
    )
    # Implementation note:
    # 1NF standards dictate ingredients and allergens should be parsed and each individual 
    # ingredient/allergen should have a row, but per the directions that step is not done
    ingredients = (
        menu_items_clean
        .select('name', 'restaurant', 'ingredients')
        .rename({'name': 'product'})
        .unique()
    )
    allergens = (
        menu_items_clean
        .select('name', 'restaurant', 'allergens')
        .rename({'name': 'product'})
        .unique()
    )
    #endregion
    
    #region Load to Database
    # TODO: set to environment variable
    db_user = 'postgres'
    db_password = 'docker'
    db_host = '0.0.0.0'
    db_port = 5432

    engine = create_engine(URL.create(
        'postgresql+pg8000',
        username=db_user,
        password=db_password,
        host=db_host,
        port=db_port,
        database='fdc',
   ))

    if not database_exists(engine.url):
        create_database(engine.url)

    if not inspect(engine).has_schema('restaurant_data'):
        with engine.connect() as conn:
            conn.execute(CreateSchema('restaurant_data', if_not_exists=True))
            conn.commit()

    Restaurant.__table__.create(bind=engine, checkfirst=True)

    restaurants.write_database('restaurant_data.restaurants', connection=engine, if_table_exists='append')

    restaurants_pk = pl.read_database('select * from restaurant_data.restaurants', connection=engine)
    restaurants_id_map =  {k:v for k,v in zip(restaurants_pk['name'], restaurants_pk['id'])}

    MenuItem.__table__.create(bind=engine, checkfirst=True)
    # add foreign key ID field
    menu_items_fk = (
        menu_items
        .with_columns(
            pl.col('restaurant')
            .replace(restaurants_id_map, return_dtype=pl.Int64)
            .alias('restaurant')
        )
        .rename({'restaurant': 'restaurant_id'})
        .sort('restaurant_id', 'name')
        .select('restaurant_id', 'name', 'category')
    )
    menu_items_fk.write_database('restaurant_data.menu_items', connection=engine, if_table_exists='append')
    menu_items_pk = pl.read_database('select * from restaurant_data.menu_items', connection=engine)

    Ingredient.__table__.create(bind=engine, checkfirst=True)
    ingredients_fk = (
        ingredients 
        .with_columns(
            pl.col('restaurant')
            .replace(restaurants_id_map, return_dtype=pl.Int64)
            .alias('restaurant')
        )
        .rename({'restaurant': 'restaurant_id'})
        .join(
            menu_items_pk,
            how='left',
            left_on=['product', 'restaurant_id'],
            right_on=['name', 'restaurant_id'],
        )
        .select(['id', 'ingredients'])
        .rename({'id': 'item_id'})
    )

    ingredients_fk.write_database('restaurant_data.ingredients', connection=engine, if_table_exists='append')

    Allergen.__table__.create(bind=engine, checkfirst=True)
    allergens_fk = (
        allergens 
        .with_columns(
            pl.col('restaurant')
            .replace(restaurants_id_map, return_dtype=pl.Int64)
            .alias('restaurant')
        )
        .rename({'restaurant': 'restaurant_id'})
        .join(
            menu_items_pk,
            how='left',
            left_on=['product', 'restaurant_id'],
            right_on=['name', 'restaurant_id'],
        )
        .select(['id', 'allergens'])
        .rename({'id': 'item_id'})
    )

    allergens_fk.write_database('restaurant_data.allergens', connection=engine, if_table_exists='append')

    Picture.__table__.create(bind=engine, checkfirst=True)
    pictures_fk = (
        pictures 
        .with_columns(
            pl.col('restaurant')
            .replace(restaurants_id_map, return_dtype=pl.Int64)
            .alias('restaurant')
        )
        .rename({'restaurant': 'restaurant_id'})
        .join(
            menu_items_pk,
            how='left',
            left_on=['product', 'restaurant_id'],
            right_on=['name', 'restaurant_id'],
        )
        .select(['id', 'picture'])
        .rename({'id': 'item_id'})
    )

    pictures_fk.write_database('restaurant_data.pictures', connection=engine, if_table_exists='append')
    
    #endregion

