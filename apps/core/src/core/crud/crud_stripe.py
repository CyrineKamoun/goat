from stripe import Customer

from core.db.models.organization import Organization
from core.deps.stripe import get_stripe
from core.utils.i18n import trans as _


class CRUDStripe:
    @staticmethod
    def get_product_metadata(plan_name: str) -> dict:
        try:
            stripe = get_stripe()
            product = stripe.Product.search(
                query=f"metadata['plan_name']:'{plan_name}'"
            )
            metadata = product.get("data")[0].get("metadata")
            return metadata
        except Exception as e:
            print(e)
            raise Exception(_("Error getting product in Stripe"))

    @staticmethod
    def create_customer(organization: Organization, email: str) -> Customer:
        try:
            stripe = get_stripe()
            customer = stripe.Customer.create(
                name=organization.name,
                email=email,
                phone=organization.phone_number,
                metadata={
                    "organization_id": str(organization.id),
                    "location": organization.location,
                },
            )
            return customer
        except Exception as e:
            print(e)
            raise Exception(_("Error creating customer in Stripe"))

    @staticmethod
    def get_stripe_plan_default_price(plan_name: str) -> str:
        try:
            stripe = get_stripe()
            product = stripe.Product.search(
                query=f"metadata['plan_name']:'{plan_name}'"
            )
            default_price = None
            if product.get("data") and len(product.get("data")) > 0:
                default_price = product.get("data")[0].get("default_price")
            if not default_price:
                raise Exception(_("Could not find a trial product"))
            return default_price
        except Exception as e:
            print(e)
            raise Exception(_("Error getting stripe plan default price"))

    @staticmethod
    def create_stripe_subscription(
        customer_id: str, price_id: str, quantity: int, trial_period_days: int
    ) -> Customer:
        try:
            stripe = get_stripe()
            subscription = stripe.Subscription.create(
                customer=customer_id,
                items=[
                    {
                        "price": price_id,
                        "quantity": quantity,
                    },
                ],
                trial_period_days=trial_period_days,
                trial_settings={"end_behavior": {"missing_payment_method": "cancel"}},
            )
            return subscription
        except Exception as e:
            print(e)
            raise Exception(_("Error creating subscription in Stripe"))


crud_stripe = CRUDStripe()
