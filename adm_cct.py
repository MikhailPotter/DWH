from confluent_kafka.admin import AdminClient, NewTopic

def adm_cct(topics):
    admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})
    admin_client.create_topics([NewTopic(topic, 1, 1) for topic in topics])

if __name__ == "__main__":
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
    adm_cct(topic_list)