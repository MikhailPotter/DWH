from confluent_kafka import Consumer
import hashlib
from datetime import date
import json
import pandas as pd
from sqlalchemy import create_engine
import datetime 

engine = create_engine('postgresql://postgres:postgres@localhost:5434/postgres')

def create_hash(obj, c_obj):
    f = ''.join(list(map(str, [obj[c] for c in c_obj])))
    return hashlib.md5(f.encode()).hexdigest()

## CATEGORY ##
def insert_category(previous_category, current_category):
    insert_hub_categories(current_category)
    if previous_category is None:
        insert_category_details(current_category)

def insert_category_details(category):
    category_pk = create_hash(category, ['category_id'])
    category_hashdiff = create_hash(category, ['category_id', 'category_name'])
    category_name = category["category_name"]

    df = pd.DataFrame.from_dict({
        "category_pk": [category_pk],
        "category_hashdiff": [category_hashdiff],
        "category_name": [category_name],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('sat_category_details', engine, if_exists="append", schema='dwh_detailed',
              index=False)

def insert_hub_categories(category):
    category_pk = create_hash(category, ['category_id'])
    category_id = category["category_id"]

    df = pd.DataFrame.from_dict({
        "category_pk": [category_pk],
        "category_id": [category_id],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('hub_categories', engine, if_exists="append", schema='dwh_detailed', index=False)

## MANUFACTURE ##
def insert_manufacture(previous_manufacture, current_manufacture):
    insert_manufacture_details(current_manufacture)
    if previous_manufacture is None:
        insert_hub_manufacturers(current_manufacture)
def insert_manufacture_details(manufacture):
    manufacturer_pk = create_hash(manufacture, ['manufacturer_id'])
    manufacturer_hashdiff = create_hash(manufacture, ['manufacturer_id', 'manufacturer_name', 'manufacturer_legal_entity'])
    manufacturer_name = manufacture["manufacturer_name"]
    manufacturer_legal_entity = manufacture["manufacturer_legal_entity"]

    df = pd.DataFrame.from_dict({
        "manufacturer_pk": [manufacturer_pk],
        "manufacturer_hashdiff": [manufacturer_hashdiff],
        "manufacturer_name": [manufacturer_name],
        "manufacturer_legal_entity": [manufacturer_legal_entity],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('sat_manufacture_details', engine, if_exists="append", schema='dwh_detailed',
              index=False)

def insert_hub_manufacturers(manufacture):
    manufacturer_pk = create_hash(manufacture, ['manufacturer_id'])
    manufacturer_id = manufacture["manufacturer_id"]

    df = pd.DataFrame.from_dict({
        "manufacturer_pk": [manufacturer_pk],
        "manufacturer_id": [manufacturer_id],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('hub_manufacturers', engine, if_exists="append", schema='dwh_detailed', index=False)

## PRODUCT ##
def insert_product_details(product):
    product_pk = hashlib.md5(str(product["product_id"]).encode()).hexdigest()
    product_hashdiff = create_hash(product, ['product_id', 'product_name', 'product_picture_url', 'product_description', 'product_restriction'])
    product_name = product["product_name"]
    product_picture_url = product["product_picture_url"]
    product_description = product["product_description"]
    product_restriction = product["product_restriction"]

    df = pd.DataFrame.from_dict({
        "product_pk": [product_pk],
        "product_hashdiff": [product_hashdiff],
        "product_name": [product_name],
        "product_picture_url": [product_picture_url],
        "product_description": [product_description],
        "product_restriction": [product_restriction],
        "product_price": [None],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('sat_product_details', engine, if_exists="append", schema='dwh_detailed',
              index=False)

def insert_hub_product(product):
    product_pk = create_hash(product, ['product_id'])

    df = pd.DataFrame.from_dict({
        "product_pk": [product_pk],
        "product_id": [product["product_id"]],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('hub_products', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_link_product_category(product):
    link_pk = create_hash(product, ['product_id', 'category_id'])
    product_pk = create_hash(product, ['product_id'])
    category_pk = create_hash(product, ['category_id'])

    df = pd.DataFrame.from_dict({
        "link_product_category_pk": [link_pk],
        "product_pk": [product_pk],
        "category_pk": [category_pk],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('link_product_category', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_link_product_manufacture(product):
    link_pk = create_hash(product, ['product_id', 'category_id'])
    product_pk = create_hash(product, ['product_id'])
    manufacturer_pk = create_hash(product, ['manufacturer_id'])

    df = pd.DataFrame.from_dict({
        "link_product_manufacture_pk": [link_pk],
        "product_pk": [product_pk],
        "manufacturer_pk": [manufacturer_pk],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('link_product_manufacture', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_product(previous_product, current_product):
    insert_product_details(current_product)
    if previous_product is None:
        insert_hub_product(current_product)
        insert_link_product_category(current_product)
        insert_link_product_manufacture(current_product)

## STORE ##
def insert_store_details(store):
    store_pk = create_hash(store, ['store_id'])
    store_hashdiff = create_hash(store, ['store_id', 'store_name', 'store_country', 'store_city', 'store_address'])
    store_name = store["store_name"]
    store_country = store["store_country"]
    store_city = store["store_city"]
    store_address = store["store_address"]
        
    df = pd.DataFrame.from_dict({
        "store_pk": [store_pk],
        "store_hashdiff": [store_hashdiff],
        "store_name": [store_name],
        "store_country": [store_country],
        "store_city": [store_city],
        "store_address": [store_address],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('sat_store_details', engine, if_exists="append", schema='dwh_detailed',
              index=False)

def insert_hub_store(store):
    store_pk = create_hash(store, ['store_id'])
    store_id = store["store_id"]

    df = pd.DataFrame.from_dict({
        "store_pk": [store_pk],
        "store_id": [store_id],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('hub_stores', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_store(previous_store, current_store):
    insert_store_details(current_store)
    if previous_store is None:
        insert_hub_store(current_store)

## CUSTOMER ##
def insert_customer_details(customer):
    customer_pk = create_hash(customer, ['customer_id'])
    customer_hashdiff = create_hash(customer, ['customer_id', 'customer_fname', 'customer_lname', 'customer_gender', 'customer_phone'])
    customer_fname = customer["customer_fname"]
    customer_lname = customer["customer_lname"]
    customer_gender = customer["customer_gender"]
    customer_phone = customer["customer_phone"]

    df = pd.DataFrame.from_dict({
        "customer_pk": [customer_pk],
        "customer_hashdiff": [customer_hashdiff],
        "customer_fname": [customer_fname],
        "customer_lname": [customer_lname],
        "customer_gender": [customer_gender],
        "customer_phone": [customer_phone],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('sat_customer_details', engine, if_exists="append", schema='dwh_detailed',
              index=False)

def insert_hub_customer(customer):
    customer_pk = create_hash(customer, ['customer_id'])
    customer_id = customer["customer_id"]

    df = pd.DataFrame.from_dict({
        "customer_pk": [customer_pk],
        "customer_id": [customer_id],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('hub_customers', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_customer(previous_customer, current_customer):
    insert_customer_details(current_customer)
    if previous_customer is None:
        insert_hub_customer(current_customer)

## PURCHASE ##
def insert_purchase(current_purchase):
    purchase_pk = create_hash(current_purchase, ['purchase_id'])
    purchase_details = get_purchase_details(purchase_pk, engine)
    old = check_purchase_existence(purchase_details)

    if (old["purchase_pk"] is None) and ("store_id" in current_purchase.keys()):
        insert_link_purchase_store(current_purchase)
        insert_link_purchase_customer(current_purchase)
        
    if (old["product_count"] is None or old["product_count"] == 0) and ("product_id" in current_purchase.keys()):
        insert_link_purchase_product(current_purchase)
    
    old.update({(k, v) for k, v in current_purchase.items() if
                k in ['product_count', 'product_price', 'purchase_id', 'purchase_date', 'purchase_payment_type']})
    
    insert_purchase_details(current_purchase, old)
    if (old["purchase_pk"] is None):
        insert_hub_purchase(current_purchase)

def get_purchase_details(purchase_pk, engine):
    sql = f"""
        select 
            *
        from dwh_detailed.sat_purchase_details
        where 1=1
            and purchase_pk = {purchase_pk} 
        order by effective_from 
        limit 1
    """
    return pd.read_sql(sql, engine).to_dict('records')

def check_purchase_existence(purchase_details):
    if len(purchase_details) == 0:
        return {
            "purchase_pk": None,
            "purchase_hashdiff": None,
            "purchase_date": None,
            "purchase_payment_type": None,
            "product_count": None,
            "product_price": None,
            "effective_from": None,
            "load_date": None,
            "record_source": None
        }
    else:
        return purchase_details[0]

def insert_link_purchase_store(purchase):
    store_pk = create_hash(purchase, ['store_id'])
    purchase_pk = create_hash(purchase, ['purchase_id'])
    link_pk = create_hash(purchase, ['store_id', 'purchase_id'])

    df = pd.DataFrame.from_dict({
        "link_store_purchase_pk": [link_pk],
        "store_pk": [store_pk],
        "purchase_pk": [purchase_pk],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('link_purchase_store', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_link_purchase_customer(purchase):
    purchase_pk = create_hash(purchase, ['purchase_id'])
    customer_pk = create_hash(purchase, ['customer_id'])
    link_pk = create_hash(purchase, ['purchase_id', 'customer_id'])

    df = pd.DataFrame.from_dict({
        "link_customer_purchase_pk": [link_pk],
        "purchase_pk": [purchase_pk],
        "customer_pk": [customer_pk],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('link_purchase_customer', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_link_purchase_product(purchase):
    purchase_pk = create_hash(purchase, ['purchase_id'])
    product_pk = create_hash(purchase, ['product_id'])
    link_pk = create_hash(purchase, ['purchase_id', 'product_id'])

    df = pd.DataFrame.from_dict({
        "link_product_purchase_pk": [link_pk],
        "purchase_pk": [purchase_pk],
        "product_pk": [product_pk],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('link_purchase_product', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_purchase_details(purchase, old):
    purchase_pk = create_hash(purchase, ['purchase_id'])
    purchase_hashdiff = create_hash(old, ['purchase_id', 'purchase_date', 'purchase_payment_type', 'product_count', 'product_price'])
    purchase_date = datetime.datetime.fromtimestamp(val // 1_000_000) if isinstance(val := old["purchase_date"], int) else old["purchase_date"]
    purchase_payment_type = old["purchase_payment_type"]
    product_count = old["product_count"]
    product_price = old["product_price"]

    df = pd.DataFrame.from_dict({
        "purchase_pk": [purchase_pk],
        "purchase_hashdiff": [purchase_hashdiff],
        "purchase_date": [purchase_date],
        "purchase_payment_type": [purchase_payment_type],
        "product_count": [product_count],
        "product_price": [product_price],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('sat_purchase_details', engine, if_exists="append", schema='dwh_detailed',
              index=False)

def insert_hub_purchase(purchase):
    purchase_pk = create_hash(purchase, ['purchase_id'])
    purchase_id = purchase["purchase_id"]

    df = pd.DataFrame.from_dict({
            "purchase_pk": [purchase_pk],
            "purchase_id": [purchase_id],
            "load_date": [date.today()],
            "record_source": ["TEST_SYSTEM"]
        })
    df.to_sql('hub_purchases', engine, if_exists="append", schema='dwh_detailed', index=False)

## DELIVERY ##
def insert_delivery(previous_delivery, current_delivery):
    insert_delivery_details(current_delivery)
    if previous_delivery is None:
        insert_hub_delivery(current_delivery)
        insert_link_product_delivery(current_delivery)

def insert_delivery_details(delivery):
    delivery_pk = create_hash(delivery, ['delivery_id'])
    delivery_hashdiff = create_hash(delivery, ['delivery_id', 'delivery_date', 'product_count'])
    delivery_date = datetime.datetime.utcfromtimestamp(0) + datetime.timedelta(delivery["delivery_date"])
    product_count = delivery["product_count"]

    df = pd.DataFrame.from_dict({
        "delivery_pk": [delivery_pk],
        "delivery_hashdiff": [delivery_hashdiff],
        "delivery_date": [delivery_date],
        "product_count": [product_count],
        "effective_from": [date.today()],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('sat_delivery_details', engine, if_exists="append", schema='dwh_detailed',
              index=False)

def insert_hub_delivery(delivery):
    delivery_pk = create_hash(delivery, ['delivery_id'])
    delivery_id = delivery["delivery_id"]

    df = pd.DataFrame.from_dict({
        "delivery_pk": [delivery_pk],
        "delivery_id": [delivery_id],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('hub_deliveries', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_link_product_delivery(delivery):
    link_pk = create_hash(delivery, ['product_id', 'delivery_id'])
    product_pk = create_hash(delivery, ['product_id'])
    delivery_pk = create_hash(delivery, ['delivery_id'])

    df = pd.DataFrame.from_dict({
        "link_product_delivery_pk": [link_pk],
        "product_pk": [product_pk],
        "delivery_pk": [delivery_pk],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('link_product_delivery', engine, if_exists="append", schema='dwh_detailed', index=False)

def insert_price_change(price):
    
    product_pk = hashlib.md5(str(price["product_id"]).encode()).hexdigest()
    old = get_product_details(product_pk, engine)[0]
    product_hashdiff = create_hash(price, ['product_id', 'product_name', 'product_picture_url', 'product_description', 'product_restriction', 'new_price'])
    product_name = old["product_name"]
    product_picture_url = old["product_picture_url"]
    product_description = old["product_description"]
    product_restriction = old["product_restriction"]
    product_price = price["new_price"]
    effective_from = datetime.datetime.fromtimestamp(price['price_change_ts'] // 1_000_000)

    df = pd.DataFrame.from_dict({
        "product_pk": [product_pk],
        "product_hashdiff": [product_hashdiff],
        "product_name": [product_name],
        "product_picture_url": [product_picture_url],
        "product_description": [product_description],
        "product_restriction": [product_restriction],
        "product_price": [product_price],
        "effective_from": [effective_from],
        "load_date": [date.today()],
        "record_source": ["TEST_SYSTEM"]
    })
    df.to_sql('sat_product_details', engine, if_exists="append", schema='dwh_detailed',
              index=False)
    
def get_product_details(product_pk, engine):
    sql = f"""
        select 
            *
        from dwh_detailed.sat_product_details 
        where 1=1
            and product_pk = '{product_pk}' 
        order by effective_from 
        limit 1
    """
    return pd.read_sql(sql, engine).to_dict('records')

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                print(f"ERROR: {msg.value()}")
            else:
                value = json.loads(msg.value().decode('utf-8'))['payload']
                prev = value.get("before", {})
                curr = value.get("after", {})
                
                if msg.topic() == "postgres.system.stores":
                    insert_store(prev, curr)
                elif msg.topic() == "postgres.system.manufacturers":
                    insert_manufacture(prev, curr)
                elif msg.topic() == "postgres.system.products":
                    insert_product(prev, curr)
                elif msg.topic() == "postgres.system.price_change":
                    insert_price_change(curr)
                elif msg.topic() == "postgres.system.purchases":
                    insert_purchase(curr)
                elif msg.topic() == "postgres.system.purchase_items":
                    insert_purchase(curr)
                elif msg.topic() == "postgres.system.categories":
                    insert_category(prev, curr)
                elif msg.topic() == "postgres.system.deliveries":
                    insert_delivery(prev, curr)
                elif msg.topic() == "postgres.system.customers":
                    insert_customer(prev, curr)
    finally:
        consumer.close()


if __name__ == "__main__":
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'dwh_group',
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'latest',
    }

    topic_list = [
        "postgres.system.categories",
        "postgres.system.customers",
        "postgres.system.deliveries",
        "postgres.system.manufacturers",
        "postgres.system.price_change",
        "postgres.system.products",
        "postgres.system.purchase_items",
        "postgres.system.purchases",
        "postgres.system.stores",
    ]
    consumer = Consumer(conf)
    basic_consume_loop(consumer, topic_list)