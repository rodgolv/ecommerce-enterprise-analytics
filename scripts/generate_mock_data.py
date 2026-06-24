import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

fake = Faker()

# Asegurar que el directorio de datos existe
os.makedirs('../data', exist_ok=True)


def generate_transactions(num_records=1000):
    data = []
    for _ in range(num_records):
        order_id = fake.uuid4()
        # Inyectar errores intencionales (10% sin email o email inválido, 5% montos negativos)
        if random.random() < 0.1:
            email = "email_invalido.com" if random.random() < 0.5 else None
        else:
            email = fake.email()

        amount = round(random.uniform(-50, 500), 2) if random.random() < 0.05 else round(random.uniform(10, 500), 2)

        # Fechas del último mes
        date = fake.date_between(start_date='-30d', end_date='today').strftime('%Y-%m-%d')

        data.append([order_id, email, amount, date])

    df = pd.DataFrame(data, columns=['order_id', 'customer_email', 'amount', 'transaction_date'])
    # Inyectar algunos duplicados
    df = pd.concat([df, df.sample(frac=0.05)])
    df.to_csv('../data/landing_transactions.csv', index=False)
    print("✅ Archivo landing_transactions.csv generado con éxito.")


def generate_marketing_data(days=30):
    data = []
    end_date = datetime.now()
    for i in range(days):
        date = (end_date - timedelta(days=i)).strftime('%Y-%m-%d')
        campaign_id = f"CAMP-{random.randint(100, 999)}"
        ad_spend = round(random.uniform(100, 1000), 2)
        clicks = int(ad_spend * random.uniform(0.5, 2.5))
        conversions = int(clicks * random.uniform(0.01, 0.1))

        data.append([campaign_id, ad_spend, clicks, conversions, date])

    df = pd.DataFrame(data, columns=['campaign_id', 'ad_spend', 'clicks', 'conversions', 'date'])
    df.to_csv('../data/campaign_performance.csv', index=False)
    print("✅ Archivo campaign_performance.csv generado con éxito.")


if __name__ == "__main__":
    print("Generando datos simulados para el pipeline...")
    generate_transactions(500)
    generate_marketing_data(30)