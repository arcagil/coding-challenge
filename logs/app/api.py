from fastapi import FastAPI, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import date
from app.models import CustomerDailyStats, engine
from sqlalchemy import select

app = FastAPI()


@app.get("/customers/{customer_id}/stats")
def get_customer_stats(
    customer_id: str, from_: date | None = Query(None, alias="from")
):
    with Session(engine) as session:
        query = select(CustomerDailyStats).where(
            CustomerDailyStats.customer_id == customer_id
        )

        if from_:
            query = query.where(CustomerDailyStats.date >= from_)

        results = session.execute(query).scalars().all()

        if not results:
            raise HTTPException(status_code=404, detail="No stats found for customer")

        return results
