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
            # Calculate daily averages and total tokens
            query = """
            SELECT 
                model_name,
                SUM(tokens) AS total_tokens,
                DATE(created_at) AS date
            FROM 
                request
            WHERE 
                created_at >= CURDATE() - INTERVAL 1 Day
                AND created_at < CURDATE()
            GROUP BY 
                model_name,
                DATE(created_at)
            """
            cursor.execute(query)
            results = cursor.fetchall()

            if not results:
                print("No data found for yesterday")
                return False

            # Insert the calculated stats into the historical stats table
            insert_query = """
            INSERT INTO daily_model_token_counts
            (date, model_name, total_tokens)
            VALUES (%s, %s, %s)
            """
            for result in results:
                cursor.execute(insert_query, result)
                print(f"Inserted daily stats for {result[0]} on {result[2]}")
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


if __name__ == "__main__":
    try:
        calculate_and_insert_daily_stats()
    finally:
        db.close()

