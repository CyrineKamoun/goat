import asyncio

from core.core.config import settings
from core.db.models import Cost
from core.db.session import session_manager

# Define the costs for each action
costs = [
    Cost(action="join", credit=30),
    Cost(action="catchment_area_active_mobility", credit=40),
    Cost(action="catchment_area_pt", credit=80),
    Cost(action="catchment_area_car", credit=80),
    Cost(action="oev_gueteklasse", credit=60),
    Cost(action="aggregate_point", credit=30),
    Cost(action="aggregate_polygon", credit=40),
    Cost(action="buffer", credit=30),
    Cost(action="trip_count_station", credit=30),
    Cost(action="origin_destination", credit=30),
    Cost(action="nearby_station_access", credit=50),
    Cost(action="heatmap_gravity_active_mobility", credit=60),
    Cost(action="heatmap_gravity_motorized_mobility", credit=100),
    Cost(action="heatmap_closest_average_active_mobility", credit=60),
    Cost(action="heatmap_closest_average_motorized_mobility", credit=100),
    Cost(action="heatmap_connectivity_active_mobility", credit=60),
    Cost(action="heatmap_connectivity_motorized_mobility", credit=100),
    Cost(action="vector_tile", credit=0.001),
]


async def main() -> None:
    session_manager.init(settings.ASYNC_SQLALCHEMY_DATABASE_URI)
    async with session_manager.session() as session:
        # Delete all costs
        await session.execute(Cost.__table__.delete())
        await session.commit()
        # Add all costs
        session.add_all(costs)
        await session.commit()


if __name__ == "__main__":
    asyncio.run(main())
