
import random
import time


PRODUCTS = {
    "Electronics": [
        ("PROD-101", "Laptop", 1499.99),
        ("PROD-102", "Airpods", 179.99),
        ("PROD-103", "Ipad", 1299.99),
    ],
    "Clothing": [
        ("PROD-201", "Hoodie", 39.99),
        ("PROD-202", "Jacket", 89.99),
        ("PROD-203", "Jeans", 49.99),
    ],
    "Appliances": [
        ("PROD-301", "Washing Machine", 499.99),
        ("PROD-302", "Refrigerator", 699.99),
        ("PROD-303", "Blender", 59.99),
    ],
}


def generate(event_number: int) -> dict:
    category = random.choice(list(PRODUCTS.keys()))
    product_id, product, price = random.choice(PRODUCTS[category])

    return {
        "order_id": f"ORD-{event_number:06d}",
        "user_id": f"USER-{random.randint(1, 500)}",
        "product_id": product_id,
        "product": product,
        "category": category,
        "quantity": random.randint(1, 10),
        "price": price,
        "event_timestamp": time.time_ns() // 1_000_000
    }


def generate_hot_key(event_number: int) -> dict:
    event = generate(event_number=event_number)

    if random.random() < 0.80:
        event["category"] = "Electronics"
        event["product_id"] = "PROD-102"
        event["product"] = "Airpods"
        event["price"] = 179.99