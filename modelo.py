import pymysql
import os
import requests
import traceback

pymysql.install_as_MySQLdb()

db = pymysql.connect(
    host=os.getenv("DATABASE_HOST"),
    user=os.getenv("DATABASE_USERNAME"),
    passwd=os.getenv("DATABASE_PASSWORD"),
    db=os.getenv("DATABASE_NAME"),
    autocommit=True,
    ssl={"ssl_ca": "/etc/ssl/certs/ca-certificates.crt"},
)

ENDON_URL = os.getenv("ENDON_URL")


def sendErrorToEndon(error: Exception, error_traceback: str, endpoint: str) -> None:
    try:
        error_payload = {
            "service": "targon-modelo",
            "endpoint": endpoint,
            "error": str(error),
            "traceback": error_traceback,
        }
        response = requests.post(
            str(ENDON_URL),
            json=error_payload,
            headers={"Content-Type": "application/json"}
        )

        if response.status_code != 200:
            print(f"Failed to report error to Endon. Status code: {response.status_code}")
            print(f"Response: {response.text}")
        else:
            print("Error successfully reported to Endon")
    except Exception as e:
        print(f"Failed to report error to Endon: {str(e)}")


def calculate_and_insert_daily_stats():
    try:
        with db.cursor() as cursor:
            # Calculate daily totals and average TPS
            query = """
            SELECT 
                DATE(created_at) AS date,
                model_name,
                SUM(response_tokens) AS total_tokens,
                AVG(response_tokens / (total_time / 1000)) as avg_tps
            FROM 
                request
            WHERE 
                created_at >= CURDATE() - INTERVAL 1 Day
                AND created_at < CURDATE()
                AND total_time > 0
                AND response_tokens > 0
            GROUP BY 
                DATE(created_at),
                model_name
            """
            cursor.execute(query)
            results = cursor.fetchall()

            if not results:
                print("No data found for yesterday")
                return False

            # Insert the calculated stats including avg_tps
            insert_query = """
            INSERT INTO daily_model_token_counts
            (created_at, model_name, total_tokens, avg_tps)
            VALUES (%s, %s, %s, %s)
            """
            for result in results:
                cursor.execute(insert_query, result)
                print(f"Inserted daily stats for {result[1]} on {result[0]} with avg TPS: {result[3]:.2f}")
            return True

    except (pymysql.Error, Exception) as e:
        error_traceback = traceback.format_exc()
        sendErrorToEndon(
            e, error_traceback, "calcorinsertion"
        )
        print(
            f"{'Database' if isinstance(e, pymysql.Error) else 'An'} error occurred: {e}"
        )
        return False

def update_historical_tps():
    with db.cursor() as cursor:
        # Get all unique dates and models
        get_dates_query = """
        SELECT DISTINCT created_at, model_name 
        FROM daily_model_token_counts
        WHERE 
            created_at >= '2024-12-12'
            AND created_at <= CURDATE()
        ORDER BY created_at
        """
        cursor.execute(get_dates_query)
        dates_and_models = cursor.fetchall()

        # Update each date/model combination
        update_query = """
        UPDATE daily_model_token_counts 
        SET avg_tps = (
            SELECT COALESCE(AVG(response_tokens / (total_time / 1000)), 0)
            FROM request
            WHERE 
                DATE(created_at) = DATE(%s)
                AND model_name = %s
                AND total_time > 0
                AND response_tokens > 0
        )
        WHERE DATE(created_at) = DATE(%s)
        AND model_name = %s
        """

        total_updates = len(dates_and_models)
        for i, (date, model) in enumerate(dates_and_models, 1):
            cursor.execute(update_query, (date, model, date, model))
            print(f"[{i}/{total_updates}] Processing {model} for {date}")

        print("Historical TPS values update completed!")

if __name__ == "__main__":
    try:
        # Calculate yesterday's stats
        if calculate_and_insert_daily_stats():
            print("Daily stats calculation completed successfully")
        else:
            print("Daily stats calculation failed")
    finally:
        db.close()

