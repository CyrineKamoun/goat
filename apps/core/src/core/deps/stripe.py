import stripe
from core.core.config import settings
from fastapi import HTTPException, Request, status

stripe.api_key = settings.STRIPE_SECRET_KEY


def get_stripe() -> "stripe":
    return stripe


async def stripe_webhook_payload(request: Request) -> stripe.Event:
    signature = request.headers.get("stripe-signature")
    data = await request.body()

    try:
        event = stripe.Webhook.construct_event(
            payload=data, sig_header=signature, secret=settings.STRIPE_WEBHOOK_SECRET
        )
        return event

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
